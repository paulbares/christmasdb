package me.paulbares.arrow;

import me.paulbares.Datastore;
import me.paulbares.dictionary.Dictionary;
import me.paulbares.dictionary.HashMapDictionary;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * -Darrow.enable_unsafe_memory_access=true --add-opens=java.base/java.nio=ALL-UNNAMED
 */
public class ArrawDatastore implements Datastore {

  protected final BufferAllocator allocator;
  protected final List<Field> fields;
  protected final Map<String, ColumnVector> fieldVectorsMap = new HashMap<>();
  protected final Map<String, Dictionary<Object>> dictionaryMap = new HashMap<>();

  protected int rowCount = 0;

  public ArrawDatastore(List<Field> fields, BufferAllocator allocator, int vectorSize) {
    this.allocator = allocator;
    this.fields = fields;
    for (Field field : fields) {
      Field f = field;
      if (field.getType() instanceof ArrowType.Utf8) {
        // Dictionarized the field
        this.dictionaryMap.put(field.getName(), new HashMapDictionary<>());
        // FIXME becareful cause f != field
        f = new Field(field.getName(), new FieldType(field.isNullable(), Types.MinorType.INT.getType(), null), null);
      }
      this.fieldVectorsMap.put(f.getName(), new ColumnVector(allocator, f, vectorSize));
    }
  }

  @Override
  public void load(String scenario, List<Object[]> tuples) {
    // TODO scenario is ignored for the moment

    int startRowCount = this.rowCount;
    for (Object[] tuple : tuples) {
      assert tuple.length == this.fields.size();

      for (int i = 0; i < tuple.length; i++) {
        Field field = this.fields.get(i);
        ColumnVector column = this.fieldVectorsMap.get(field.getName());
        Dictionary<Object> dictionary = this.dictionaryMap.get(field.getName());
        if (dictionary == null) {
          column.writeObject(this.rowCount, tuple[i]);
        } else {
          int position = dictionary.map(tuple[i]);
          column.writeInt(this.rowCount, position);
        }
      }
      this.rowCount++;
    }

    setValueCount(startRowCount, tuples.size());
  }

  protected void setValueCount(int startRowCount, int nbOfTuples) {
    // After loading, set the values vectors that have been written
    for (Field field : this.fields) {
      this.fieldVectorsMap.get(field.getName()).setValueCount(startRowCount, nbOfTuples);
    }
  }

  @Override
  public StructField[] getFields() {
    return new StructField[0];
  }

  public String contentToTSVString() {
    StringBuilder sb = new StringBuilder();
    List<Object> row = new ArrayList<>(this.fields.size() + 1);
    row.add("line");
    for (Field field : this.fields) {
      row.add(field.getName());
    }
    printRow(sb, row);
    int rowCount = this.rowCount;
    for (int i = 0; i < rowCount; i++) {
      row.clear();
      row.add(i);
      for (Field field : this.fields) {
         this.fieldVectorsMap.get(field.getName());
        row.add(this.fieldVectorsMap.get(field.getName()).getObject(i));
      }
      printRow(sb, row);
    }
    return sb.toString();
  }

  private void printRow(StringBuilder sb, List<Object> row) {
    boolean first = true;
    for (Object v : row) {
      if (first) {
        first = false;
      } else {
        sb.append("\t");
      }
      sb.append(v);
    }
    sb.append("\n");
  }

  public static void main(String[] args) {
    RootAllocator allocator = new RootAllocator();
    List<Field> fields = new ArrayList<>();

    int id = 0;
    fields.add(new Field("name", new FieldType(false, new ArrowType.Utf8(), new DictionaryEncoding(id++, false, null)), null));
    fields.add(new Field("age", new FieldType(false, new ArrowType.Int(8, false), null), null));
    fields.add(new Field("height", new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null));

    ArrawDatastore arrawDatastore = new ArrawDatastore(fields, allocator, 4);

    List<Object[]> tuples = Arrays.asList(
            new Object[] {"paul", 34, 192d},
            new Object[] {"peter", 16, 165d},
            new Object[] {"john", 65, 172.6d},
            new Object[] {"mary", 3, 72d},
            new Object[] {"bob", 42, 182d},
            new Object[] {"jack", 30, 175d}
    );
    arrawDatastore.load("", tuples);
    arrawDatastore.load("", tuples);
//    arrawDatastore.load("", tuples);
//    arrawDatastore.load("", tuples);
    System.out.println(arrawDatastore.contentToTSVString());
  }
}

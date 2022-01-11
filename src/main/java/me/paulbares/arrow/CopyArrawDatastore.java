package me.paulbares.arrow;

import me.paulbares.Datastore;
import me.paulbares.dictionary.Dictionary;
import me.paulbares.dictionary.HashMapDictionary;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
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
public class CopyArrawDatastore implements Datastore {

  /**
   * MacOS: $sysctl hw.l1dcachesize to know the cache size. It is often better for applications to work in chunks
   * fitting in CPU cache. For that reason, let's use a good cache friendly default chunk size.
   * <p>
   * for me, the cache size is 32k.
   * </p>
   */
  public static final int DEFAULT_VECTOR_SIZE = 4; // 8192 = 1 << 13

  protected final int vectorSize;
  protected final BufferAllocator allocator;
  protected final List<Field> fields;
  protected final Map<Field, FieldVector[]> fieldVectorsMap = new HashMap<>();
  protected final Map<Field, Dictionary<Object>> dictionaryMap = new HashMap<>();

  protected final int logSize;
  protected final int sizeMinus1;
  protected int rowCount = 0;

  public CopyArrawDatastore(List<Field> fields, BufferAllocator allocator, int vectorSize) {
    if (!isPowerOfTwo(vectorSize)) {
      throw new IllegalArgumentException(vectorSize + "not a power of 2");
    }
    this.logSize = Integer.numberOfTrailingZeros(vectorSize);
    this.sizeMinus1 = vectorSize - 1;

    this.allocator = allocator;
    this.vectorSize = vectorSize;
    this.fields = fields;
    for (Field field : fields) {
      FieldVector[] v = new FieldVector[]{createVector(field, true)};
      this.fieldVectorsMap.put(field, v);
    }
  }

  public static boolean isPowerOfTwo(int number) {
    return number > 0 && ((number & (number - 1)) == 0);
  }

  protected FieldVector createVector(Field field, boolean createDictionary) {
    ArrowType type = field.getType();
    FieldVector v;
    if (type instanceof ArrowType.Utf8) {
      if (createDictionary) {
        // Dictionarized the field
        this.dictionaryMap.put(field, new HashMapDictionary<>());
      }
      v = Types.MinorType.INT.getNewVector(field, this.allocator, null);
    } else if (!(type instanceof ArrowType.Int
            && type instanceof ArrowType.FloatingPoint
            && type instanceof ArrowType.Bool)) {
      v = field.createVector(this.allocator);
    } else {
      throw new RuntimeException(String.format("Type not supported %s", type));
    }
    ((FixedWidthVector) v).allocateNew(this.vectorSize); // only support fixed width vector. if not => dictionarized
    return v;
  }

  @Override
  public void load(String scenario, List<Object[]> tuples) {
    // TODO scenario is ignored for the moment

    int startRowCount = this.rowCount;
    for (Object[] tuple : tuples) {
      assert tuple.length == this.fields.size();

      int bucket = this.rowCount >> this.logSize;
      int offset = this.rowCount & this.sizeMinus1;

      for (int i = 0; i < tuple.length; i++) {
        Field field = this.fields.get(i);
        FieldVector v = getVectorOrCreateNew(field, bucket);
        Dictionary<Object> dictionary = this.dictionaryMap.get(field);
        if (dictionary == null) {
          writeValue(v, offset, tuple[i]);
        } else {
          int position = dictionary.map(tuple[i]);
          ((IntVector) v).set(offset, position);
        }
      }
      this.rowCount++;
    }

    setValueCount(startRowCount, tuples.size());
  }

  protected FieldVector getVectorOrCreateNew(Field field, int bucket) {
    FieldVector[] fieldVectors = this.fieldVectorsMap.get(field);
    FieldVector v;
    if (bucket >= fieldVectors.length) {
      FieldVector[] newFieldVectors = new FieldVector[fieldVectors.length + 1];
      System.arraycopy(fieldVectors, 0, newFieldVectors, 0, fieldVectors.length);
      newFieldVectors[fieldVectors.length] = (v = createVector(field, false));
      this.fieldVectorsMap.put(field, newFieldVectors);
    } else {
      v = fieldVectors[bucket];
    }
    return v;
  }

  protected void setValueCount(int startRowCount, int nbOfTuples) {
    // After loading, set the values vectors that have been written
    int startBucket = startRowCount >> this.logSize;
    for (Field field : this.fields) {
      FieldVector[] fieldVectors = this.fieldVectorsMap.get(field);
      for (int i = startBucket; i < fieldVectors.length - 1; i++) {
        fieldVectors[i].setValueCount(this.vectorSize); // mark all full
      }
      fieldVectors[fieldVectors.length - 1].setValueCount(nbOfTuples >> this.logSize); // for the last
    }
  }

  protected void writeValue(FieldVector vector, int index, Object obj) {
    if (vector instanceof IntVector v) {
      v.set(index, (int) obj);
    } else if (vector instanceof UInt1Vector v) {
      v.set(index, (int) obj);
    } else if (vector instanceof UInt2Vector v) {
      v.set(index, (int) obj);
    } else if (vector instanceof UInt4Vector v) {
      v.set(index, (int) obj);
    } else if (vector instanceof UInt8Vector v) {
      v.set(index, (int) obj);
    } else if (vector instanceof TinyIntVector v) {
      v.set(index, (int) obj);
    } else if (vector instanceof SmallIntVector v) {
      v.set(index, (int) obj);
    } else if (vector instanceof BigIntVector v) {
      v.set(index, (long) obj);
    } else if (vector instanceof Float8Vector v) {
      v.set(index, (double) obj);
    } else {
      throw new RuntimeException(String.format("Unsupported field %s", vector.getField()));
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
      int bucket = i >> this.logSize;
      int offset = i & this.sizeMinus1;
      for (Field field : this.fields) {
        FieldVector v = this.fieldVectorsMap.get(field)[bucket];
        row.add(v.getObject(offset));
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

    CopyArrawDatastore arrawDatastore = new CopyArrawDatastore(fields, allocator, 4);

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

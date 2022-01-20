package me.paulbares.arrow;

import me.paulbares.Datastore;
import me.paulbares.dictionary.Dictionary;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * -Darrow.enable_unsafe_memory_access=true --add-opens=java.base/java.nio=ALL-UNNAMED
 */
public class ArrawDatastore implements Datastore {

  protected final BufferAllocator allocator;
  protected final int vectorSize;
  protected final List<Field> fields;
  protected final Map<String, ColumnVector> fieldVectorsMap = new HashMap<>();
  protected final DictionaryProvider dictionaryProvider = new DictionaryProvider();
  protected final int keyIndex;
  protected final SingleValuePrimaryIndex primaryIndex;
  protected final Map<String, Map<String, ColumnVector>> vectorByFieldByScenario = new HashMap<>();
  protected final Map<String, Map<String, MutableIntIntMap>> rowMappingByFieldByScenario = new HashMap<>();

  protected int rowCount = 0;

  public ArrawDatastore(List<Field> fields, int[] keyIndices, int vectorSize) {
    this(fields, keyIndices, new RootAllocator(), vectorSize);
  }

  public ArrawDatastore(List<Field> fields, int[] keyIndices, BufferAllocator allocator, int vectorSize) {
    if (keyIndices.length > 1) {
      throw new IllegalArgumentException("Not supported, currently only single-value primary index is supported");
    }
    this.vectorSize = vectorSize;
    this.allocator = allocator;
    this.fields = fields;
    this.keyIndex = keyIndices[0];
    this.primaryIndex = new SingleValuePrimaryIndex(this.dictionaryProvider, this.fields.get(this.keyIndex));
    for (Field field : fields) {
      this.fieldVectorsMap.put(field.getName(), createColumnVector(field));
    }
  }

  protected ColumnVector createColumnVector(Field field) {
    Field f = field;
    if (field.getType() instanceof ArrowType.Utf8) {
      // Dictionarized the field.
      this.dictionaryProvider.getOrCreate(field.getName());
      f = new Field(field.getName(), new FieldType(field.isNullable(), Types.MinorType.INT.getType(), null), null);
    }
    return new ColumnVector(this.allocator, f, this.vectorSize);
  }

  @Override
  public void load(String scenario, List<Object[]> tuples) {
    this.dictionaryProvider.getOrCreate(scenario).map(scenario);

    int startRowCount = this.rowCount;
    for (Object[] tuple : tuples) {
      assert tuple.length == this.fields.size();

      if (scenario.equals(MAIN_SCENARIO_NAME)) {
        for (int i = 0; i < tuple.length; i++) {
          Field field = this.fields.get(i);
          ColumnVector column = this.fieldVectorsMap.get(field.getName());
          Dictionary<Object> dictionary = this.dictionaryProvider.get(field.getName());
          if (dictionary == null) {
            column.setObject(this.rowCount, tuple[i]);
          } else {
            int position = dictionary.map(tuple[i]);
            column.setInt(this.rowCount, position);
          }
        }

        this.primaryIndex.mapKey(tuple[this.keyIndex], this.rowCount);
      } else {
        int row = this.primaryIndex.getRow(tuple[this.keyIndex]); // it is supposed to exist
        if (row < 0) {
          throw new IllegalArgumentException("Cannot find key " + tuple[this.keyIndex] + " in " + MAIN_SCENARIO_NAME + " scenario");
        }
        // Find the fields that are different.

        for (int i = 0; i < tuple.length; i++) {
          Field field = this.fields.get(i);
          ColumnVector column = this.fieldVectorsMap.get(field.getName());
          Dictionary<Object> dictionary = this.dictionaryProvider.get(field.getName());

          boolean isEqual;
          if (dictionary == null) {
            Object o = column.getObject(row);
            isEqual = o.equals(tuple[i]);
          } else {
            int o = column.getInt(row);
            int position = dictionary.getPosition(tuple[i]);
            isEqual = o == position;
          }

          if (!isEqual) {
            ColumnVector vector = this.vectorByFieldByScenario
                    .computeIfAbsent(scenario, k -> new HashMap<>())
                    .computeIfAbsent(field.getName(), k -> createColumnVector(field));

            int targetRow;
            if (dictionary == null) {
              targetRow = vector.appendObject(tuple[i]);
            } else {
              int position = dictionary.map(tuple[i]);
              targetRow = vector.appendInt(position);
            }
            this.rowMappingByFieldByScenario.computeIfAbsent(scenario, k -> new HashMap<>())
                    .computeIfAbsent(field.getName(), k -> new IntIntHashMap())
                    .put(row, targetRow);
          }
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

  public static void printRow(StringBuilder sb, List<Object> row) {
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
}

package me.paulbares.arrow;

import me.paulbares.dictionary.Dictionary;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TesArrowDatastore {

  @Test
  void testLoading() {
    RootAllocator allocator = new RootAllocator();
    List<Field> fields = new ArrayList<>();

    int id = 0;
    fields.add(new Field("name", new FieldType(false, new ArrowType.Utf8(), new DictionaryEncoding(id++, false, null)), null));
    fields.add(new Field("age", new FieldType(false, new ArrowType.Int(8, false), null), null));
    fields.add(new Field("height", new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null));

    ArrawDatastore datastore = new ArrawDatastore(fields, allocator, 4);

    // Make sure there are more elements than the vector size
    List<Object[]> tuples = Arrays.asList(
            new Object[] {"paul", 34, 192d},
            new Object[] {"peter", 16, 165d},
            new Object[] {"john", 65, 172.6d},
            new Object[] {"mary", 3, 72d},
            new Object[] {"bob", 42, 182d},
            new Object[] {"jack", 30, 175d}
    );

    datastore.load(null, tuples);

    for (int row = 0; row < tuples.size(); row++) {
      for (int i = 0; i < tuples.get(row).length; i++) {
        Dictionary<Object> dic = datastore.dictionaryMap.get(fields.get(i).getName());
        if (dic != null) {
          Object read = dic.read(datastore.fieldVectorsMap.get(fields.get(i).getName()).getInt(row));
          Assertions.assertThat(read).isEqualTo(tuples.get(row)[i]);
        } else {
          Assertions.assertThat(datastore.fieldVectorsMap.get(fields.get(i).getName()).getObject(row)).isEqualTo(tuples.get(row)[i]);
        }
      }
    }
  }
}

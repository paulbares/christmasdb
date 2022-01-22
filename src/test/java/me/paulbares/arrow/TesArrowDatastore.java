package me.paulbares.arrow;

import me.paulbares.Datastore;
import me.paulbares.dictionary.Dictionary;
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
    List<Field> fields = new ArrayList<>();

    int id = 0;
    fields.add(new Field("name", new FieldType(false, new ArrowType.Utf8(), new DictionaryEncoding(id++, false, null)), null));
    fields.add(new Field("age", new FieldType(false, new ArrowType.Int(8, false), null), null));
    fields.add(new Field("height", new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null));

    ArrawDatastore datastore = new ArrawDatastore(fields, new int[]{0}, 4);

    // Make sure there are more elements than the vector size
    List<Object[]> tuples = Arrays.asList(
            new Object[] {"paul", 34, 192d},
            new Object[] {"peter", 16, 165d},
            new Object[] {"john", 65, 172.6d},
            new Object[] {"mary", 3, 72d},
            new Object[] {"bob", 42, 182d},
            new Object[] {"jack", 30, 175d}
    );

    datastore.load(Datastore.MAIN_SCENARIO_NAME, tuples);

    for (int row = 0; row < tuples.size(); row++) {
      for (int i = 0; i < tuples.get(row).length; i++) {
        Dictionary<Object> dic = datastore.dictionaryProvider.get(fields.get(i).getName());
        if (dic != null) {
          Object read = dic.read(datastore.vectorByFieldByScenario.get(Datastore.MAIN_SCENARIO_NAME).get(fields.get(i).getName()).getInt(row));
          Assertions.assertThat(read).isEqualTo(tuples.get(row)[i]);
        } else {
          Assertions.assertThat(datastore.vectorByFieldByScenario.get(Datastore.MAIN_SCENARIO_NAME).get(fields.get(i).getName()).getObject(row)).isEqualTo(tuples.get(row)[i]);
        }
      }
    }
  }

  @Test
  void testLoadingScenarios() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("id", new FieldType(false, new ArrowType.Int(8, true), null), null));
    fields.add(new Field("product", new FieldType(false, new ArrowType.Utf8(), null), null));
    fields.add(new Field("price", new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null));

    ArrawDatastore datastore = new ArrawDatastore(fields, new int[]{0}, 4);

    List<Object[]> tuples = Arrays.asList(
            new Object[] {0, "syrup", 2d},
            new Object[] {1, "tofu", 8d},
            new Object[] {2, "mozzarella", 4d}
    );
    datastore.load(Datastore.MAIN_SCENARIO_NAME, tuples);

    // Scenario 1
    List<Object[]> tuplesScenario1 = Arrays.asList(
            new Object[] {0, "syrup", 3d},
            new Object[] {1, "tofu", 6d}
    );
    datastore.load("s1", tuplesScenario1);

    // Scenario 2
    List<Object[]> tuplesScenario2 = Arrays.asList(
            new Object[] {0, "syrup", 4d},
            new Object[] {2, "mozzarella", 5d}
    );
    datastore.load("s2", tuplesScenario2);

    // FIXME assert content??
  }
}

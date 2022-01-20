package me.paulbares.arrow;

import me.paulbares.aggregation.SumAggregator;
import me.paulbares.query.PointListAggregateResult;
import me.paulbares.query.Query;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestQuery {

  static ArrawDatastore datastore;

  @BeforeAll
  static void beforeAll() {
    List<Field> fields = new ArrayList<>();

    int id = 0;
    fields.add(new Field("name", new FieldType(false, new ArrowType.Utf8(), new DictionaryEncoding(id++, false, null)), null));
    fields.add(new Field("country", new FieldType(false, new ArrowType.Utf8(), new DictionaryEncoding(id++, false, null)), null));
    fields.add(new Field("age", new FieldType(false, new ArrowType.Int(8, false), null), null));
    fields.add(new Field("height", new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null));

    datastore = new ArrawDatastore(fields, new int[]{0}, 4);

    // Make sure there are more elements than the vector size
    List<Object[]> tuples = Arrays.asList(
            new Object[] {"paul", "france", 1, 1d},
            new Object[] {"peter", "england", 1, 1d},
            new Object[] {"john", "usa", 2, 2d},
            new Object[] {"mary", "france", 4, 4d},
            new Object[] {"bob", "usa", 8, 8d},
            new Object[] {"jack", "usa", 1, 1d}
    );

    datastore.load(null, tuples);
  }

  @Test
  void testWildcardSingleCoordinate() {
    Query query = new Query()
            .addWildcardCoordinate("country")
            .addAggregatedMeasure("age", SumAggregator.TYPE)
            .addAggregatedMeasure("height", SumAggregator.TYPE);

    PointListAggregateResult result = new ArrowQueryEngine(datastore).execute(query);
    Assertions.assertThat(result.size()).isEqualTo(3);
    Assertions.assertThat(result.getAggregates(List.of("england"))).containsExactly(1l, 1d);
    Assertions.assertThat(result.getAggregates(List.of("usa"))).containsExactly(11l, 11d);
    Assertions.assertThat(result.getAggregates(List.of("france"))).containsExactly(5l, 5d);
  }

  @Test
  void testWildcardTwoCoordinates() {
    Query query = new Query()
            .addWildcardCoordinate("name")
            .addWildcardCoordinate("country")
            .addAggregatedMeasure("age", SumAggregator.TYPE)
            .addAggregatedMeasure("height", SumAggregator.TYPE);

    PointListAggregateResult result = new ArrowQueryEngine(datastore).execute(query);
    Assertions.assertThat(result.size()).isEqualTo(6);
    Assertions.assertThat(result.getAggregates(List.of("paul", "france"))).containsExactly(1l, 1d);
    Assertions.assertThat(result.getAggregates(List.of("peter", "england"))).containsExactly(1l, 1d);
    Assertions.assertThat(result.getAggregates(List.of("john", "usa"))).containsExactly(2l, 2d);
    Assertions.assertThat(result.getAggregates(List.of("mary", "france"))).containsExactly(4l, 4d);
    Assertions.assertThat(result.getAggregates(List.of("bob", "usa"))).containsExactly(8l, 8d);
    Assertions.assertThat(result.getAggregates(List.of("jack", "usa"))).containsExactly(1l, 1d);
  }

  @Test
  void testListCoordinates() {
    Query query = new Query()
            .addCoordinates("name", "paul", "john", "mary")
            .addCoordinates("country", "france", "usa")
            .addAggregatedMeasure("age", SumAggregator.TYPE)
            .addAggregatedMeasure("height", SumAggregator.TYPE);

    PointListAggregateResult result = new ArrowQueryEngine(datastore).execute(query);
    Assertions.assertThat(result.size()).isEqualTo(3);
    Assertions.assertThat(result.getAggregates(List.of("paul", "france"))).containsExactly(1l, 1d);
    Assertions.assertThat(result.getAggregates(List.of("john", "usa"))).containsExactly(2l, 2d);
    Assertions.assertThat(result.getAggregates(List.of("mary", "france"))).containsExactly(4l, 4d);
    Assertions.assertThat(result.getAggregates(List.of("mary", "usa"))).isNull();
  }

  @Test
  void testMixWildcardAndListCoordinates() {
    Query query = new Query()
            .addCoordinates("name", "paul", "john")
            .addWildcardCoordinate("country")
            .addAggregatedMeasure("age", SumAggregator.TYPE)
            .addAggregatedMeasure("height", SumAggregator.TYPE);

    PointListAggregateResult result = new ArrowQueryEngine(datastore).execute(query);
    System.out.println(result);
    Assertions.assertThat(result.size()).isEqualTo(2);
    Assertions.assertThat(result.getAggregates(List.of("paul", "france"))).containsExactly(1l, 1d);
    Assertions.assertThat(result.getAggregates(List.of("john", "usa"))).containsExactly(2l, 2d);
  }
}

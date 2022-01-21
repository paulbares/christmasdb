package me.paulbares.arrow;

import me.paulbares.Datastore;
import me.paulbares.aggregation.SumAggregator;
import me.paulbares.query.PointListAggregateResult;
import me.paulbares.query.Query;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestQueryScenario {

  static ArrawDatastore datastore;

  @BeforeAll
  static void beforeAll() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("id", new FieldType(false, new ArrowType.Int(8, true), null), null));
    fields.add(new Field("product", new FieldType(false, new ArrowType.Utf8(), null), null));
    fields.add(new Field("price", new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null));

    datastore = new ArrawDatastore(fields, new int[]{0}, 4);

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
  }

  @Test
  void testWildcardWithoutScenarioInTheQuery() {
    Query query = new Query()
            .addWildcardCoordinate("product")
            .addAggregatedMeasure("price", SumAggregator.TYPE);

    PointListAggregateResult result = new ArrowQueryEngineScenario(datastore).execute(query);
    Assertions.assertThat(result.size()).isEqualTo(3);
    Assertions.assertThat(result.getAggregates(List.of("syrup"))).containsExactly(2d);
    Assertions.assertThat(result.getAggregates(List.of("tofu"))).containsExactly(8d);
    Assertions.assertThat(result.getAggregates(List.of("mozzarella"))).containsExactly(4d);
  }

  @Test
  void testWildcardScenarioOnly() {
    Query query = new Query()
            .addWildcardCoordinate(Datastore.SCENARIO_FIELD)
            .addAggregatedMeasure("price", SumAggregator.TYPE);

    PointListAggregateResult result = new ArrowQueryEngineScenario(datastore).execute(query);
    Assertions.assertThat(result.size()).isEqualTo(3);
    Assertions.assertThat(result.getAggregates(List.of("base"))).containsExactly(14d);
    Assertions.assertThat(result.getAggregates(List.of("s1"))).containsExactly(13d);
    Assertions.assertThat(result.getAggregates(List.of("s2"))).containsExactly(17d);
  }

  @Test
  void testWildcardTwoCoordinates() {
    Query query = new Query()
            .addWildcardCoordinate(Datastore.SCENARIO_FIELD)
            .addWildcardCoordinate("product")
            .addAggregatedMeasure("price", SumAggregator.TYPE);

    PointListAggregateResult result = new ArrowQueryEngineScenario(datastore).execute(query);
    Assertions.assertThat(result.size()).isEqualTo(9);
    Assertions.assertThat(result.getAggregates(List.of("base", "syrup"))).containsExactly(2d);
    Assertions.assertThat(result.getAggregates(List.of("base", "tofu"))).containsExactly(8d);
    Assertions.assertThat(result.getAggregates(List.of("base", "mozzarella"))).containsExactly(4d);
    Assertions.assertThat(result.getAggregates(List.of("s1", "syrup"))).containsExactly(3d);
    Assertions.assertThat(result.getAggregates(List.of("s1", "tofu"))).containsExactly(6d);
    Assertions.assertThat(result.getAggregates(List.of("s1", "mozzarella"))).containsExactly(4d);
    Assertions.assertThat(result.getAggregates(List.of("s2", "syrup"))).containsExactly(4d);
    Assertions.assertThat(result.getAggregates(List.of("s2", "tofu"))).containsExactly(8d);
    Assertions.assertThat(result.getAggregates(List.of("s2", "mozzarella"))).containsExactly(5d);
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

package me.paulbares;

import me.paulbares.query.Query;
import me.paulbares.query.QueryEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static me.paulbares.Datastore.MAIN_SCENARIO_NAME;

public class TestQueryEngine {

  static Datastore ds;

  @BeforeAll
  static void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);
    ds = new Datastore(List.of(ean, category, price, qty));

    ds.load(MAIN_SCENARIO_NAME, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    ds.load("s1", List.of(
            new Object[]{"bottle", "drink", 4d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    ds.load("s2", List.of(
            new Object[]{"bottle", "drink", 1.5d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  @Test
  void testQueryWildcard() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum");
    List<Row> collect = new QueryEngine(ds).executeSparkSql(query).collectAsList();
    Assertions.assertThat(collect).containsExactlyInAnyOrder(
            RowFactory.create("base", 15.0d, 33),
            RowFactory.create("s1", 17.0d, 33),
            RowFactory.create("s2", 14.5d, 33));
  }

  @Test
  void testQuerySeveralCoordinates() {
    Query query = new Query()
            .addCoordinates("scenario", "s1", "s2")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum");
    List<Row> collect = new QueryEngine(ds).executeSparkSql(query).collectAsList();
    Assertions.assertThat(collect).containsExactlyInAnyOrder(
            RowFactory.create("s1", 17.0d, 33),
            RowFactory.create("s2", 14.5d, 33));
  }

  @Test
  void testQuerySingleCoordinate() {
    Query query = new Query()
            .addSingleCoordinate("scenario", "s1")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum");
    Dataset<Row> rowDataset = new QueryEngine(ds).executeSparkSql(query);
    List<Row> collect = rowDataset.collectAsList();
    Assertions.assertThat(collect).containsExactlyInAnyOrder(
            RowFactory.create("s1", 17.0d, 33));
  }

  /**
   * Without measure, we can use it to do a discovery.
   */
  @Test
  void testDiscovery() {
    Query query = new Query().addWildcardCoordinate("scenario");
    Dataset<Row> rowDataset = new QueryEngine(ds).executeSparkSql(query);
    List<Row> collect = rowDataset.collectAsList();
    Assertions.assertThat(collect)
            .containsExactlyInAnyOrder(
                    RowFactory.create(MAIN_SCENARIO_NAME),
                    RowFactory.create("s1"),
                    RowFactory.create("s2")
            );
  }
}

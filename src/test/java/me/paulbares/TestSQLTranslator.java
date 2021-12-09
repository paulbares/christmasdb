package me.paulbares;

import me.paulbares.query.Query;
import me.paulbares.query.SQLTranslator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestSQLTranslator {

  @Test
  void testGrandTotal() {
    Query query = new Query()
            .addMeasure("pnl", "sum")
            .addMeasure("delta", "sum")
            .addMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select sum(pnl), sum(delta), avg(pnl) from base_store");
  }

  @Test
  void testGroupBy() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addWildcardCoordinate("type")
            .addMeasure("pnl", "sum")
            .addMeasure("delta", "sum")
            .addMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select scenario, type, sum(pnl), sum(delta), avg(pnl) from base_store group by scenario, type");
  }

  @Test
  void testSingleConditionSingleField() {
    Query query = new Query()
            .addSingleCoordinate("scenario", "Base")
            .addWildcardCoordinate("type")
            .addMeasure("pnl", "sum")
            .addMeasure("delta", "sum")
            .addMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select scenario, type, sum(pnl), sum(delta), avg(pnl) from base_store where scenario = 'Base' group by scenario, type");

  }

  @Test
  void testConditionsSeveralField() {
    Query query = new Query()
            .addSingleCoordinate("scenario", "Base")
            .addCoordinates("type", List.of("A", "B"))
            .addMeasure("pnl", "sum")
            .addMeasure("delta", "sum")
            .addMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select scenario, type, sum(pnl), sum(delta), avg(pnl) from base_store where scenario = 'Base' and type in ('A, B') group by scenario, type");
  }
}

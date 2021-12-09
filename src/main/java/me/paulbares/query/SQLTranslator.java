package me.paulbares.query;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SQLTranslator {

//   datastore.get()
//           .groupBy("Scenario", type.getName())
//          .agg(sum(col("Marge")), sum(col("NumérateurIndice")), sum(score.col()))

  // TODO support only groupBy agg

  //  SELECT column_name(s)
//  FROM table_name
//  WHERE condition
//  GROUP BY column_name(s)
//  record
  public static String translate(Query query) {
    List<String> selects = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    List<String> conditions = new ArrayList<>();

    List<String> aggregates = new ArrayList<>();
    query.coordinates.forEach((field, values) -> {
      groupBy.add(field);
      if (values == null) {
        // wildcard
      } else if (values.size() == 1) {
        conditions.add(field + " = '" + values.get(0) + "'");
      } else {
        conditions.add(field + " in (" + values.stream().collect(Collectors.joining(", ", "'", "'")) + ")");
      }
    });
    query.measure.forEach(m -> aggregates.add(m.aggregationFunction + "(" + m.name + ")"));

    groupBy.forEach(selects::add); // coord first, then aggregates
    aggregates.forEach(selects::add);

    StringBuilder statement = new StringBuilder();
    statement.append("select ");
    statement.append(selects.stream().collect(Collectors.joining(", ")));
    statement.append(" from base_store");
    if (!conditions.isEmpty()) {
      statement.append(" where ").append(conditions.stream().collect(Collectors.joining(" and ")));
    }
    if (!groupBy.isEmpty()) {
      statement.append(" group by ").append(groupBy.stream().collect(Collectors.joining(", ")));
    }
    return statement.toString();
  }
}

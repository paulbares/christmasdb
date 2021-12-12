package me.paulbares.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class Query {

  public static final AtomicLong ID = new AtomicLong();

  public Map<String, List<String>> coordinates = new LinkedHashMap<>();

  public List<Measure> measures = new ArrayList<>();

  public final long id;

  public Query() {
    this.id = ID.getAndIncrement();
  }

  public Query addWildcardCoordinate(String field) {
    this.coordinates.put(field, null);
    return this;
  }

  public Query addSingleCoordinate(String field, String value) {
    this.coordinates.put(field, List.of(value));
    return this;
  }

  public Query addCoordinates(String field, String first, String... others) {
    List<String> values = new ArrayList<>();
    values.add(first);
    values.addAll(Arrays.stream(others).toList());
    this.coordinates.put(field, values);
    return this;
  }

  public Query addAggregatedMeasure(String field, String agg) {
    this.measures.add(new AggregatedMeasure(field, agg));
    return this;
  }

  public Query addExpressionMeasure(String expression) {
    this.measures.add(new ExpressionMeasure(expression));
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o){
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Query query = (Query) o;
    return id == query.id && coordinates.equals(query.coordinates) && measures.equals(query.measures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(coordinates, measures, id);
  }

  @Override
  public String toString() {
    return "Query{" +
            "id=" + id +
            ", coordinates=" + coordinates +
            ", measure=" + measures +
            '}';
  }
}

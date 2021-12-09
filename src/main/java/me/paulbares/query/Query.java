package me.paulbares.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Query {

  public static final String WILDCARD = "__*__";

  public Map<String, List<String>> coordinates = new LinkedHashMap<>();

  public List<Measure> measure = new ArrayList<>();

  public Query addWildcardCoordinate(String field) {
    this.coordinates.put(field, null);
    return this;
  }

  public Query addSingleCoordinate(String field, String value) {
    this.coordinates.put(field, List.of(value));
    return this;
  }

  public Query addCoordinates(String field, List<String> values) {
    this.coordinates.put(field, values);
    return this;
  }

  public Query addMeasure(String field, String agg) {
    this.measure.add(new Measure(field, agg));
    return this;
  }
}

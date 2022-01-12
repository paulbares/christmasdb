package me.paulbares.query;

import java.util.List;

public interface QueryResult {

  List<Object> getAggregates(Object[] coordinates, String... aggregates);

  List<Object> getAggregates(List<Object> coordinates, String... aggregates);

  int size();
}

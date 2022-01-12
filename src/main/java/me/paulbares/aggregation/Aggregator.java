package me.paulbares.aggregation;

import me.paulbares.arrow.ColumnVector;

public interface Aggregator {

  void aggregate(int sourcePosition, int destinationPosition);

  ColumnVector getDestination();
}

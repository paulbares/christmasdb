package me.paulbares.query;

import java.util.Objects;

import static me.paulbares.query.SQLTranslator.escape;

public class AggregatedMeasure implements Measure {

  public String name;

  public String aggregationFunction;

  /**
   * For jackson.
   */
  public AggregatedMeasure() {
  }

  public AggregatedMeasure(String name, String aggregationFunction) {
    this.name = Objects.requireNonNull(name);
    this.aggregationFunction = Objects.requireNonNull(aggregationFunction);
  }

  @Override
  public String sqlExpression() {
    return this.aggregationFunction + "(" + escape(this.name) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AggregatedMeasure that = (AggregatedMeasure) o;
    return name.equals(that.name) && aggregationFunction.equals(that.aggregationFunction);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return "AggregatedMeasure{" +
            "name='" + name + '\'' +
            ", aggregationFunction='" + aggregationFunction + '\'' +
            '}';
  }
}

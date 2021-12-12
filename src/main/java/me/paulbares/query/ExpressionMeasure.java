package me.paulbares.query;

import java.util.Objects;

public class ExpressionMeasure implements Measure {

  public String expression;

  /**
   * For jackson.
   */
  public ExpressionMeasure() {
  }

  public ExpressionMeasure(String expression) {
    this.expression = Objects.requireNonNull(expression);
  }

  @Override
  public String sqlExpression() {
    return this.expression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpressionMeasure that = (ExpressionMeasure) o;
    return expression.equals(that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression);
  }

  @Override
  public String toString() {
    return "ExpressionMeasure{" +
            "expression='" + expression + '\'' +
            '}';
  }
}

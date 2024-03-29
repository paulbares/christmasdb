package me.paulbares.aggregation;

import me.paulbares.arrow.ColumnVector;
import me.paulbares.arrow.ImmutableColumnVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class SumAggregator {

  public static final String TYPE = "sum";

  public abstract static class ASumAggregator implements Aggregator {

    protected final ImmutableColumnVector source;
    protected final ColumnVector destination;

    public ASumAggregator(ImmutableColumnVector source, ColumnVector destination) {
      this.source = source;
      this.destination = destination;
      checkSource();
      checkDestination();
    }

    @Override
    public ColumnVector getDestination() {
      return this.destination;
    }

    protected void checkSource() {
      // NOOP
    }

    protected void checkDestination() {
      // NOOP
    }
  }

  public static class SumIntAggregator extends ASumAggregator {

    public SumIntAggregator(ImmutableColumnVector source, ColumnVector destination) {
      super(source, destination);
    }

    @Override
    protected void checkSource() {
      if (this.source.getField().getFieldType().getType() instanceof ArrowType.Int type) {
        if (type.getBitWidth() > 32) {
          throw new IllegalArgumentException("Incorrect bit width " + type);
        }
      } else {
        throw new IllegalArgumentException("Incorrect type " + this.source.getField().getFieldType().getType());
      }
    }

    @Override
    protected void checkDestination() {
      if (this.destination.getField().getFieldType().getType() instanceof ArrowType.Int destType) {
        if (destType.getBitWidth() != 64) {
          throw new IllegalArgumentException("Incorrect bit width " + destType);
        }
      }
    }

    @Override
    public void aggregate(int sourcePosition, int destinationPosition) {
      int a = this.source.getInt(sourcePosition);
      long b = this.destination.getLong(destinationPosition);
      this.destination.setLong(destinationPosition, a + b);
    }
  }

  public static class SumDoubleAggregator extends ASumAggregator {

    public SumDoubleAggregator(ImmutableColumnVector source, ColumnVector destination) {
      super(source, destination);
    }

    @Override
    protected void checkSource() {
      checkType(this.source);
    }

    @Override
    protected void checkDestination() {
      checkType(this.destination);
    }

    private void checkType(ImmutableColumnVector col) {
      if (col.getField().getFieldType().getType() instanceof ArrowType.FloatingPoint type) {
        if (type.getPrecision() != FloatingPointPrecision.DOUBLE) {
          throw new IllegalArgumentException("Incorrect floating point precision " + type.getPrecision());
        }
      } else {
        throw new IllegalArgumentException("Incorrect type " + col.getField().getFieldType().getType());
      }
    }

    @Override
    public void aggregate(int sourcePosition, int destinationPosition) {
      double a = this.source.getDouble(sourcePosition);
      double b = this.destination.getDouble(destinationPosition);
      this.destination.setDouble(destinationPosition, Double.sum(a, b));
    }
  }
}

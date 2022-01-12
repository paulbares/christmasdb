package me.paulbares.aggregation;

import me.paulbares.arrow.ColumnVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class AggregatorFactory {

  public static Aggregator create(String aggregationType, ColumnVector source, ColumnVector destination) {
    if (aggregationType.equals(SumAggregator.TYPE)) {
      if (source.getField().getFieldType().getType() instanceof ArrowType.Int) {
        return new SumAggregator.SumIntAggregator(source, destination);
      } else if (source.getField().getFieldType().getType() instanceof ArrowType.FloatingPoint) {
        return new SumAggregator.SumDoubleAggregator(source, destination);
      } else {
        throw new IllegalArgumentException(String.format("Unsupported input source type for %s, type: %s ", aggregationType, source.getField().getFieldType().getType()));
      }
    } else {
      throw new IllegalArgumentException("Unsupported aggregation type " + aggregationType);
    }
  }

  public static Aggregator create(BufferAllocator allocator, ColumnVector source, String aggregationType, String destinationColumnName) {
    if (aggregationType.equals(SumAggregator.TYPE)) {
      if (source.getField().getFieldType().getType() instanceof ArrowType.Int) {
        ColumnVector destination = createLongColumnVector(allocator, destinationColumnName);
        return new SumAggregator.SumIntAggregator(source, destination);
      } else if (source.getField().getFieldType().getType() instanceof ArrowType.FloatingPoint) {
        ColumnVector destination = createDoubleColumnVector(allocator, destinationColumnName);
        return new SumAggregator.SumDoubleAggregator(source, destination);
      } else {
        throw new IllegalArgumentException(String.format("Unsupported input source type for %s, type: %s ", aggregationType, source.getField().getFieldType().getType()));
      }
    } else {
      throw new IllegalArgumentException("Unsupported aggregation type " + aggregationType);
    }
  }

  public static ColumnVector createDoubleColumnVector(BufferAllocator allocator, String name) {
    return new ColumnVector(allocator, Field.nullable(name, Types.MinorType.FLOAT8.getType()));
  }

  public static ColumnVector createLongColumnVector(BufferAllocator allocator, String name) {
    return new ColumnVector(allocator, Field.nullable(name, Types.MinorType.BIGINT.getType()));
  }
}

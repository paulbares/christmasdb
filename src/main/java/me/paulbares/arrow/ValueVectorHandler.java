package me.paulbares.arrow;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;

public abstract class ValueVectorHandler {

  public abstract ValueVector getValueVector();

  public int getInt(int index) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public void setInt(int index, int value) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public Object getObject(int index) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public void setObject(int index, Object value) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public void setLong(int index, long value) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public double getDouble(int index) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public void setDouble(int index, double value) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public static class UInt1VectorHandler extends ValueVectorHandler {

    protected final UInt1Vector vector;

    public UInt1VectorHandler(UInt1Vector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public int getInt(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setInt(index, (int) value);
    }
  }

  public static class IntVectorHandler extends ValueVectorHandler {

    protected final IntVector vector;

    public IntVectorHandler(IntVector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public int getInt(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setInt(index, (int) value);
    }
  }

  public static class UInt2VectorHandler extends ValueVectorHandler {

    protected final UInt2Vector vector;

    public UInt2VectorHandler(UInt2Vector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public int getInt(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setInt(index, (int) value);
    }
  }

  public static class UInt4VectorHandler extends ValueVectorHandler {

    protected final UInt4Vector vector;

    public UInt4VectorHandler(UInt4Vector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public int getInt(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setInt(index, (int) value);
    }
  }

  public static class UInt8VectorHandler extends ValueVectorHandler {

    protected final UInt8Vector vector;

    public UInt8VectorHandler(UInt8Vector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public long getLong(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setInt(index, (int) value);
    }
  }

  public static class TinyIntVectorHandler extends ValueVectorHandler {

    protected final TinyIntVector vector;

    public TinyIntVectorHandler(TinyIntVector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public int getInt(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setInt(index, (int) value);
    }
  }

  public static class SmallIntVectorHandler extends ValueVectorHandler {

    protected final SmallIntVector vector;

    public SmallIntVectorHandler(SmallIntVector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public int getInt(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setInt(index, (int) value);
    }
  }

  public static class BigIntVectorHandler extends ValueVectorHandler {

    protected final BigIntVector vector;

    public BigIntVectorHandler(BigIntVector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public long getLong(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setLong(int index, long value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getLong(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setLong(index, (Long) value);
    }
  }

  public static class Float8VectorHandler extends ValueVectorHandler {

    protected final Float8Vector vector;

    public Float8VectorHandler(Float8Vector vector) {
      this.vector = vector;
    }

    @Override
    public ValueVector getValueVector() {
      return this.vector;
    }

    @Override
    public double getDouble(int index) {
      return this.vector.get(index);
    }

    @Override
    public void setDouble(int index, double value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getDouble(index);
    }

    @Override
    public void setObject(int index, Object value) {
      setDouble(index, (double) value);
    }
  }
}
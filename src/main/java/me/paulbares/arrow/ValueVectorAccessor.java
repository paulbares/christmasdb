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

public abstract class ValueVectorAccessor {

  public abstract ValueVector getValueVector();

  public int getInt(int index) {
    throw new UnsupportedOperationException();
  }

  public void writeInt(int index, int value) {
    throw new UnsupportedOperationException();
  }

  public Object getObject(int index) {
    throw new UnsupportedOperationException();
  }

  public void writeObject(int index, Object value) {
    throw new UnsupportedOperationException();
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException();
  }

  public double getDouble(int index) {
    throw new UnsupportedOperationException();
  }

  public void writeDouble(int index, double value) {
    throw new UnsupportedOperationException();
  }

  public static class UInt1VectorAccessor extends ValueVectorAccessor {

    protected final UInt1Vector vector;

    public UInt1VectorAccessor(UInt1Vector vector) {
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
    public void writeInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void writeObject(int index, Object value) {
      writeInt(index, (int) value);
    }
  }

  public static class IntVectorAccessor extends ValueVectorAccessor {

    protected final IntVector vector;

    public IntVectorAccessor(IntVector vector) {
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
    public void writeInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void writeObject(int index, Object value) {
      writeInt(index, (int) value);
    }
  }

  public static class UInt2VectorAccessor extends ValueVectorAccessor {

    protected final UInt2Vector vector;

    public UInt2VectorAccessor(UInt2Vector vector) {
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
    public void writeInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void writeObject(int index, Object value) {
      writeInt(index, (int) value);
    }
  }

  public static class UInt4VectorAccessor extends ValueVectorAccessor {

    protected final UInt4Vector vector;

    public UInt4VectorAccessor(UInt4Vector vector) {
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
    public void writeInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void writeObject(int index, Object value) {
      writeInt(index, (int) value);
    }
  }

  public static class UInt8VectorAccessor extends ValueVectorAccessor {

    protected final UInt8Vector vector;

    public UInt8VectorAccessor(UInt8Vector vector) {
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
    public void writeInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void writeObject(int index, Object value) {
      writeInt(index, (int) value);
    }
  }

  public static class TinyIntVectorAccessor extends ValueVectorAccessor {

    protected final TinyIntVector vector;

    public TinyIntVectorAccessor(TinyIntVector vector) {
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
    public void writeInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void writeObject(int index, Object value) {
      writeInt(index, (int) value);
    }
  }

  public static class SmallIntVectorAccessor extends ValueVectorAccessor {

    protected final SmallIntVector vector;

    public SmallIntVectorAccessor(SmallIntVector vector) {
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
    public void writeInt(int index, int value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getInt(index);
    }

    @Override
    public void writeObject(int index, Object value) {
      writeInt(index, (int) value);
    }
  }

  public static class BigIntVectorAccessor extends ValueVectorAccessor {

    protected final BigIntVector vector;

    public BigIntVectorAccessor(BigIntVector vector) {
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
  }

  public static class Float8VectorAccessor extends ValueVectorAccessor {

    protected final Float8Vector vector;

    public Float8VectorAccessor(Float8Vector vector) {
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
    public void writeDouble(int index, double value) {
      this.vector.set(index, value);
    }

    @Override
    public Object getObject(int index) {
      return getDouble(index);
    }

    @Override
    public void writeObject(int index, Object value) {
      writeDouble(index, (double) value);
    }
  }
}
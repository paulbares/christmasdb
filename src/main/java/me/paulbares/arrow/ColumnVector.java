package me.paulbares.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class ColumnVector {

  /**
   * MacOS: $sysctl hw.l1dcachesize to know the cache size. It is often better for applications to work in chunks
   * fitting in CPU cache. For that reason, let's use a good cache friendly default chunk size.
   * <p>
   * for me, the cache size is 32k.
   * </p>
   */
  public static final int DEFAULT_VECTOR_SIZE = 4; // 8192 = 1 << 13

  protected final BufferAllocator allocator;

  protected ValueVectorHandler[] accessors;

  protected final Field field;
  protected final int log2Size;
  protected final int sizeMinusOne;
  protected final int vectorSize;

  public ColumnVector(BufferAllocator allocator, Field field, int vectorSize) {
    if (!isPowerOfTwo(vectorSize)) {
      throw new IllegalArgumentException(vectorSize + "not a power of 2");
    }
    this.allocator = allocator;
    this.field = field;
    this.vectorSize = vectorSize;
    this.log2Size = Integer.numberOfTrailingZeros(vectorSize);
    this.sizeMinusOne = vectorSize - 1;
    this.accessors = new ValueVectorHandler[]{createAccessor(field)};
  }

  public static boolean isPowerOfTwo(int number) {
    return number > 0 && ((number & (number - 1)) == 0);
  }

  // TODO move elsewhere
  protected void setValueCount(int startRowCount, int nbOfTuples) {
    // After loading, set the values vectors that have been written
    int startBucket = startRowCount >> this.log2Size;
    for (int i = startBucket; i < this.accessors.length - 1; i++) {
      this.accessors[i].getValueVector().setValueCount(this.vectorSize); // mark all full
    }
    this.accessors[this.accessors.length - 1].getValueVector().setValueCount(nbOfTuples >> this.log2Size); // for the last element
  }

  private ValueVectorHandler newAccessor(ValueVector vector) {
    if (vector instanceof IntVector v) {
      return new ValueVectorHandler.IntVectorHandler(v);
    } else if (vector instanceof UInt1Vector v) {
      return new ValueVectorHandler.UInt1VectorHandler(v);
    } else if (vector instanceof UInt2Vector v) {
      return new ValueVectorHandler.UInt2VectorHandler(v);
    } else if (vector instanceof UInt4Vector v) {
      return new ValueVectorHandler.UInt4VectorHandler(v);
    } else if (vector instanceof UInt8Vector v) {
      return new ValueVectorHandler.UInt8VectorHandler(v);
    } else if (vector instanceof TinyIntVector v) {
      return new ValueVectorHandler.TinyIntVectorHandler(v);
    } else if (vector instanceof SmallIntVector v) {
      return new ValueVectorHandler.SmallIntVectorHandler(v);
    } else if (vector instanceof BigIntVector v) {
      return new ValueVectorHandler.BigIntVectorHandler(v);
    } else if (vector instanceof Float8Vector v) {
      return new ValueVectorHandler.Float8VectorHandler(v);
    } else {
      throw new RuntimeException(String.format("Unsupported field %s", vector.getField()));
    }
  }

  protected ValueVectorHandler createAccessor(Field field) {
    ArrowType type = field.getType();
    FieldVector v;
    if (!(type instanceof ArrowType.Int
            && type instanceof ArrowType.FloatingPoint
            && type instanceof ArrowType.Bool)) {
      v = field.createVector(this.allocator);
    } else {
      throw new RuntimeException(String.format("Type not supported %s", type));
    }
    ((FixedWidthVector) v).allocateNew(this.vectorSize); // only support fixed width vector. if not => dictionarized
    return newAccessor(v);
  }

  public void writeInt(int index, int value) {
    getBucket(index >> this.log2Size).writeInt(index & this.sizeMinusOne, value);
  }

  public void writeObject(int index, Object value) {
    getBucket(index >> this.log2Size).writeObject(index & this.sizeMinusOne, value);
  }

  public int getInt(int index) {
    int bucket = index >> this.log2Size;
    int offset = index & this.sizeMinusOne;
    return this.accessors[bucket].getInt(offset);
  }

  public Object getObject(int index) {
    int bucket = index >> this.log2Size;
    int offset = index & this.sizeMinusOne;
    return this.accessors[bucket].getObject(offset);
  }

  private ValueVectorHandler getBucket(int bucket) {
    ValueVectorHandler v;
    if (bucket >= this.accessors.length) {
      ValueVectorHandler[] newFieldVectors = new ValueVectorHandler[this.accessors.length + 1];
      System.arraycopy(this.accessors, 0, newFieldVectors, 0, this.accessors.length);
      newFieldVectors[this.accessors.length] = (v = createAccessor(this.field));
      this.accessors = newFieldVectors;
    } else {
      v = this.accessors[bucket];
    }
    return v;
  }
}

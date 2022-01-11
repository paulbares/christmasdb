package me.paulbares.dictionary;

import jdk.internal.util.ArraysSupport;
import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMapWithHashingStrategy;

import java.util.Arrays;

public class PointDictionary {

  /**
   * The value to indicate no value
   */
  private static final int FREE = -1;

  protected final ObjectIntHashMapWithHashingStrategy<int[]> underlyingDic;
  protected final int pointLength;

  public PointDictionary(int pointLength) {
    this(pointLength, 16);
  }

  public PointDictionary(int pointLength, int initialCapacity) {
    this.underlyingDic = new ObjectIntHashMapWithHashingStrategy<>(IntegerArrayHashingStrategy.INSTANCE, initialCapacity);
    this.pointLength = pointLength;
  }

  /**
   * The length of the array should be the same as {@link #pointLength}. No check is made.
   *
   * @param value
   * @return
   */
  public int map(int[] value) {
    assert value.length == this.pointLength;
    int size = this.underlyingDic.size();
    return this.underlyingDic.getIfAbsentPut(value, size);
  }

  /**
   * Gets the position of the key in the dictionary.
   *
   * @param key the key to find
   * @return position of the key in the dictionary, or -1 if the key is not in the dictionary.
   */
  public int getPosition(int[] key) {
    return this.underlyingDic.getIfAbsent(key, FREE);
  }

  public int getPointLength() {
    return this.pointLength;
  }

//  public void forEach(PointProcedure procedure) {
//    this.underlyingDic.forEachKeyValue((points, row) -> procedure.execute(points, row));
//  }

  public int size() {
    return this.underlyingDic.size();
  }

  private static final class IntegerArrayHashingStrategy implements HashingStrategy<int[]> {
    private static final long serialVersionUID = 1L;

    private static HashingStrategy<int[]> INSTANCE = new IntegerArrayHashingStrategy();

    @Override
    public int computeHashCode(int[] object) {
      return Arrays.hashCode(object); // FIXME poor hash function
    }

    @Override
    public boolean equals(int[] object1, int[] object2) {
      // Use this function because faster. Not check is done regarding the size
      return ArraysSupport.mismatch(object1, object2, object1.length) < 0;
    }
  }
}

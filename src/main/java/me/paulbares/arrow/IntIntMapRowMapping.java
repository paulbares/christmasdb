package me.paulbares.arrow;

import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

public class IntIntMapRowMapping implements RowMapping {

  protected final MutableIntIntMap mapping = new IntIntHashMap();

  @Override
  public void map(int row, int targetRow) {
      this.mapping.put(row, targetRow);
  }

  @Override
  public int get(int row) {
    return this.mapping.getIfAbsent(row, -1);
  }
}

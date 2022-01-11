package me.paulbares.dictionary;


import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

public class HashMapDictionary<K> implements Dictionary<K>{

  private ObjectIntHashMap<K> map = new ObjectIntHashMap();
  private IntObjectHashMap<K> map2 = new IntObjectHashMap();

  @Override
  public int map(K value) {
    int size = map.size();
    int pos = map.getIfAbsentPut(value, size);
    map2.put(pos, value); // TODO FIXME
    return pos;
  }

  @Override
  public K read(int position) {
    return map2.get(position);
  }

  @Override
  public int getPosition(K value) {
    return map.getIfAbsent(value, -1);
  }
}

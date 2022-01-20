package me.paulbares.arrow;

import me.paulbares.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

public class SingleValuePrimaryIndex {

  private static final int ABSENT = -1;

  protected final Field keyField;
  protected final Dictionary<Object> dic;
  protected final MutableIntIntMap lines = new IntIntHashMap();

  public SingleValuePrimaryIndex(DictionaryProvider dictionaryProvider, Field keyField) {
    if (!(keyField.getType() instanceof ArrowType.Int)) {
      if (keyField.getType() instanceof ArrowType.Utf8) {
        // support only strings
      } else {
        throw new IllegalArgumentException("Type not supported " + keyField.getType());
      }
    }
    this.keyField = keyField;
    this.dic = dictionaryProvider.getOrCreate(keyField.getName());
  }

  public void mapKey(Object key, int line) {
    int pos = this.dic.map(key);
    int ifAbsent = getRow(pos);
    if (ifAbsent < 0) {
      this.lines.put(pos, line);
    } else {
      // Already map, this might be an issue
      throw new RuntimeException("A record for the key " + key + " already exist line " + ifAbsent);
    }
  }

  public int getRow(Object key) {
    int pos = this.dic.map(key);
    return getRow(pos);
  }

  public int getRow(int key) {
    return key >= 0 ? this.lines.getIfAbsent(key, ABSENT) : ABSENT;
  }
}

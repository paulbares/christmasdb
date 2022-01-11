package me.paulbares.dictionary;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDictionary {

  @Test
  void test() {
    Dictionary<Object> dic = new HashMapDictionary<>();

    int i = 0;
    Assertions.assertEquals(i++, dic.map("a"));
    Assertions.assertEquals(i++, dic.map("b"));
    Assertions.assertEquals(i++, dic.map("c"));
    Assertions.assertEquals(i++, dic.map("d"));
    Assertions.assertEquals(i++, dic.map("e"));

    i = 0;
    Assertions.assertEquals(i++, dic.getPosition("a"));
    Assertions.assertEquals(i++, dic.getPosition("b"));
    Assertions.assertEquals(i++, dic.getPosition("c"));
    Assertions.assertEquals(i++, dic.getPosition("d"));
    Assertions.assertEquals(i++, dic.getPosition("e"));
    Assertions.assertEquals(-1, dic.getPosition("unknown"));

    i = 0;
    Assertions.assertEquals("a", dic.read(i++));
    Assertions.assertEquals("b", dic.read(i++));
    Assertions.assertEquals("c", dic.read(i++));
    Assertions.assertEquals("d", dic.read(i++));
    Assertions.assertEquals("e", dic.read(i++));
    Assertions.assertEquals(null, dic.read(i));
    Assertions.assertEquals(null, dic.read(i + 2));
    Assertions.assertEquals(null, dic.read(100));
  }
}

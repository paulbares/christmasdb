package me.paulbares.arrow;

import me.paulbares.dictionary.Dictionary;
import me.paulbares.dictionary.HashMapDictionary;

import java.util.HashMap;
import java.util.Map;

public class DictionaryProvider {

  protected final Map<String, Dictionary<Object>> dictionaryMap = new HashMap<>();

  public Dictionary<Object> getOrCreate(String fieldName) {
    return this.dictionaryMap.computeIfAbsent(fieldName, k -> new HashMapDictionary<>());
  }

  public Dictionary<Object> get(String fieldName) {
    return this.dictionaryMap.get(fieldName);
  }
}

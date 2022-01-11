package me.paulbares.dictionary;

public interface Dictionary<K> {

  int map(K value);

  K read(int position);

  int getPosition(K value);
}

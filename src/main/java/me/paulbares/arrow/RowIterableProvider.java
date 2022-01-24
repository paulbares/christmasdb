package me.paulbares.arrow;

import java.util.function.Function;

public interface RowIterableProvider extends Function<String, IntIterable> {

  IntIterable get(String scenario);

  @Override
  default IntIterable apply(String s) {
    return get(s);
  }
}

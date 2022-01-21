package me.paulbares.arrow;

import org.apache.arrow.vector.types.pojo.Field;

public interface ImmutableColumnVector {

  int getInt(int index);

  double getDouble(int index);

  Object getObject(int index);

  long getLong(int index);

  Field getField();
}
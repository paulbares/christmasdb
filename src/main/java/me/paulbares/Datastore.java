package me.paulbares;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

public interface Datastore {

  String BASE_STORE_NAME = "base_store";
  String MAIN_SCENARIO_NAME = "base";

  Field[] getFields(); // FIXME should not be spark fields

  void load(String scenario, List<Object[]> tuples);
}

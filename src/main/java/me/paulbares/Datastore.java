package me.paulbares;

import java.util.List;

public interface Datastore {

  String MAIN_SCENARIO_NAME = "base";
  String SCENARIO_FIELD = "scenario";

  void load(String scenario, List<Object[]> tuples);
}

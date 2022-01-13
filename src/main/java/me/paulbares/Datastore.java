package me.paulbares;

import java.util.List;

public interface Datastore {

  void load(String scenario, List<Object[]> tuples);
}

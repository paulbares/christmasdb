package me.paulbares.query;

public class Measure {

  public String name;

  public String aggregationFunction;

  public Measure(String name, String aggregationFunction) {
    this.name = name;
    this.aggregationFunction = aggregationFunction;
  }
}

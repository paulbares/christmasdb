package me.paulbares.arrow;

public interface RowMapping {

  void map(int row, int targetRow);

  int get(int row);
}

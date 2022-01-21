package me.paulbares.arrow;

public class IdentityMapping implements RowMapping {

  public static final RowMapping SINGLETON = new IdentityMapping();

  private IdentityMapping() {
  }

  @Override
  public void map(int row, int targetRow) {
    // NOOP
  }

  @Override
  public int get(int row) {
    return row;
  }
}

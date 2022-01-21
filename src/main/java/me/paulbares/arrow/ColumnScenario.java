package me.paulbares.arrow;

import org.apache.arrow.vector.types.pojo.Field;

/**
 * TODO should be rename in VectorScenarioReader???
 *
 * FIXME to avoid reading the mapping to know where to read the value, could we use a bitmap? Will it be quicker?
 */
public class ColumnScenario implements ImmutableColumnVector {

  protected final ColumnVector scenarioVector;
  protected final ColumnVector baseVector;
  protected final String scenario;
  protected final RowMapping mapping;

  public ColumnScenario(ColumnVector baseVector, ColumnVector scenarioVector, String scenario, RowMapping mapping) {
    this.baseVector = baseVector;
    this.scenarioVector = scenarioVector;
    this.scenario = scenario;
    this.mapping = mapping;
  }

  @Override
  public int getInt(int baseRow) {
    int scenarioRow = this.mapping.get(baseRow);
    if (scenarioRow == -1) {
      return this.baseVector.getInt(baseRow);
    } else {
      return this.scenarioVector.getInt(scenarioRow);
    }
  }

  @Override
  public long getLong(int baseRow) {
    int scenarioRow = this.mapping.get(baseRow);
    if (scenarioRow == -1) {
      return this.baseVector.getLong(baseRow);
    } else {
      return this.scenarioVector.getLong(scenarioRow);
    }
  }

  @Override
  public double getDouble(int baseRow) {
    int scenarioRow = this.mapping.get(baseRow);
    if (scenarioRow == -1) {
      return this.baseVector.getDouble(baseRow);
    } else {
      return this.scenarioVector.getDouble(scenarioRow);
    }
  }

  @Override
  public Object getObject(int baseRow) {
    int scenarioRow = this.mapping.get(baseRow);
    if (scenarioRow == -1) {
      return this.baseVector.getObject(baseRow);
    } else {
      return this.scenarioVector.getObject(scenarioRow);
    }
  }

  @Override
  public Field getField() {
    return this.scenarioVector.getField();
  }
}

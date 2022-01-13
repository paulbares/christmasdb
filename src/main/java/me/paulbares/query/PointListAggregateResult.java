package me.paulbares.query;

import me.paulbares.arrow.ColumnVector;
import me.paulbares.dictionary.Dictionary;
import me.paulbares.dictionary.PointDictionary;

import java.util.ArrayList;
import java.util.List;

import static me.paulbares.arrow.ArrawDatastore.printRow;

public class PointListAggregateResult implements QueryResult {

  protected final PointDictionary pointDictionary;

  private final List<String> pointNames;

  private final List<ColumnVector> aggregates;

  private final List<Dictionary<Object>> dictionaries;

  public PointListAggregateResult(PointDictionary pointDictionary,
                                  List<String> pointNames, // align with point dic
                                  List<Dictionary<Object>> dictionaries,
                                  List<ColumnVector> aggregates) {
    this.pointDictionary = pointDictionary;
    this.pointNames = pointNames;
    this.dictionaries = dictionaries;
    this.aggregates = aggregates;
  }

  @Override
  public int size() {
    return this.pointDictionary.size();
  }

  @Override
  public List<Object> getAggregates(Object[] coordinates, String... aggregates) {
    int[] buffer = new int[this.pointDictionary.getPointLength()];
    for (int i = 0; i < coordinates.length; i++) {
        buffer[i] = this.dictionaries.get(i).getPosition(coordinates[i]);
    }
    int position = this.pointDictionary.getPosition(buffer);
    if (position < 0) {
      return null;
    } else {
      // FIXME aggregates arg is ignored right now
      List<Object> res = new ArrayList<>(this.aggregates.size());
      for (ColumnVector aggregate : this.aggregates) {
        res.add(aggregate.getObject(position));
      }
      return res;
    }
  }

  @Override
  public List<Object> getAggregates(List<Object> coordinates, String... aggregates) {
    return getAggregates(coordinates.toArray(new Object[0]), aggregates);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    this.pointDictionary.forEach((points, row) -> {
      List<Object> r = new ArrayList<>(this.pointNames.size() + 1);
      for (int i = 0; i < points.length; i++) {
        r.add(this.dictionaries.get(i).read(points[i]));
      }

      for (ColumnVector aggregate : this.aggregates) {
        r.add(aggregate.getObject(row));
      }
      printRow(sb, r);
    });
    return sb.toString();
  }
}

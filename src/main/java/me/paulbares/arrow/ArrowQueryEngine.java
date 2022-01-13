package me.paulbares.arrow;

import me.paulbares.aggregation.Aggregator;
import me.paulbares.aggregation.AggregatorFactory;
import me.paulbares.dictionary.Dictionary;
import me.paulbares.dictionary.PointDictionary;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.PointListAggregateResult;
import me.paulbares.query.Query;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ArrowQueryEngine {

  protected final ArrawDatastore store;

  public ArrowQueryEngine(ArrawDatastore store) {
    this.store = store;
  }

  public PointListAggregateResult execute(Query query) {
    Map<String, MutableIntSet> acceptedValuesByField = new LinkedHashMap<>();

    List<ColumnVector> pointVectors = new ArrayList<>();
    query.coordinates.forEach((field, values) -> {
      if (values == null) {
        // wildcard, all values are accepted
        pointVectors.add(this.store.fieldVectorsMap.get(field));
      } else {
        pointVectors.add(this.store.fieldVectorsMap.get(field));
        for (String value : values) {
          Field f = Schema.findField(this.store.fields, field);
          Dictionary<Object> dictionary = this.store.dictionaryMap.get(f.getName());
          int position = dictionary.getPosition(value);
          if (position >= 0) {
            acceptedValuesByField.computeIfAbsent(f.getName(), key -> new IntHashSet()).add(position);
          }
        }
      }
    });

    List<Aggregator> aggregators = new ArrayList<>();
    query.measures.forEach(measure -> {
      if (measure instanceof AggregatedMeasure agg) {
        aggregators.add(AggregatorFactory.create(
                this.store.allocator,
                this.store.fieldVectorsMap.get(agg.field),
                agg.aggregationFunction,
                agg.sqlExpression()));
      } else {
        throw new RuntimeException("Not implemented yet");
      }
    });

    RoaringBitmap matchRows = null;
    if (acceptedValuesByField.isEmpty()) {
      // All lines are accepted
      matchRows = null;
    } else {
      // Take the first field that will be used as reference to build the first version of matching rows. Currently,
      // the same field (first one) is chosen, but we can imagine having a proper algorithm to choose it and try to
      // reduce the number of bit set into the map to accelerate any subsequent operations.
      matchRows = initializeBitmap(acceptedValuesByField);
      applyConditionsOnBitmap(matchRows, acceptedValuesByField);
    }

    PointDictionary pointDictionary = new PointDictionary(pointVectors.size());

    IntConsumer rowAggregator = row -> {
      // TODO do by batch? to read the same column multiple times
      // Fill the buffer
      int j = 0;
      int[] buffer = new int[pointVectors.size()];
      for (ColumnVector c : pointVectors) {
        buffer[j++] = c.getInt(row);
      }

      int destinationRow = pointDictionary.map(buffer);
      // And then aggregate
      boolean check = false; // to do it only once
      for (Aggregator aggregator : aggregators) {
        if (!check){
          aggregator.getDestination().ensureCapacity(destinationRow);
        }

        aggregator.aggregate(row, destinationRow);
      }
    };

    if (matchRows != null) {
      matchRows.forEach(rowAggregator);
    } else {
      for (int i = 0; i < this.store.rowCount; i++) {
        rowAggregator.accept(i);
      }
    }

    List<String> pointNames = pointVectors.stream().map(v -> v.getField().getName()).collect(Collectors.toList());
    return new PointListAggregateResult(
            pointDictionary,
            pointNames,
            pointNames.stream().map(pointName -> this.store.dictionaryMap.get(pointName)).collect(Collectors.toList()),
            aggregators.stream().map(Aggregator::getDestination).collect(Collectors.toList()));
  }

  /**
   * Creates the first {@link RoaringBitmap} to use. Current impl. takes the first element.
   */
  protected RoaringBitmap initializeBitmap(Map<String, MutableIntSet> acceptedValuesByField) {
    RoaringBitmap matchRows = new RoaringBitmap();
    List<String> fields = new ArrayList<>(acceptedValuesByField.keySet());
    String refField = fields.get(0);
    IntSet refValues = acceptedValuesByField.get(refField);
    ColumnVector refColumnVector = this.store.fieldVectorsMap.get(refField);
    for (int row = 0; row < this.store.rowCount; row++) {
      if (refValues.contains(refColumnVector.getInt(row))) {
        matchRows.add(row);
      }
    }
    return matchRows;
  }

  /**
   * Edits the input bitmap by combining the other conditions.
   */
  protected void applyConditionsOnBitmap(RoaringBitmap matchRows, Map<String, MutableIntSet> acceptedValuesByField) {
    List<String> fields = new ArrayList<>(acceptedValuesByField.keySet());
    for (int i = 1; i < fields.size(); i++) {
      String field = fields.get(i);
      IntSet values = acceptedValuesByField.get(field);
      ColumnVector vector = this.store.fieldVectorsMap.get(field);

      RoaringBitmap tmp = new RoaringBitmap();
      matchRows.forEach((org.roaringbitmap.IntConsumer) row -> {
        if (values.contains(vector.getInt(row))) {
          tmp.add(row);
        }
      });
      matchRows.and(tmp);
    }
  }
}

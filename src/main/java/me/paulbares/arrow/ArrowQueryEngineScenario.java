package me.paulbares.arrow;

import me.paulbares.Datastore;
import me.paulbares.aggregation.Aggregator;
import me.paulbares.aggregation.AggregatorFactory;
import me.paulbares.dictionary.Dictionary;
import me.paulbares.dictionary.PointDictionary;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.PointListAggregateResult;
import me.paulbares.query.Query;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A different version compatible with scenario.
 */
public class ArrowQueryEngineScenario {

  protected final ArrawDatastore store;

  public ArrowQueryEngineScenario(ArrawDatastore store) {
    this.store = store;
  }

  public PointListAggregateResult execute(Query query) {
    Map<String, MutableIntSet> acceptedValuesByField = computeAcceptedValues(query);
    IntList queriedScenarios = computeQueriedScenarios(query);
    List<ColumnVector> aggregates = new ArrayList<>();
    Map<String, List<Aggregator>> aggregatorsByScenario = computeAggregators(query, queriedScenarios, aggregates);

    int pointSize = query.coordinates.size();
    PointDictionary pointDictionary = new PointDictionary(pointSize);
    List<String> pointNames = FastList.newList(query.coordinates.keySet());

    int scenarioIndex = new ArrayList<>(query.coordinates.keySet()).indexOf(Datastore.SCENARIO_FIELD);
    int[] scenariosArray = queriedScenarios.toArray();
    RowIterableProvider rowIterableProvider = RowIterableProviderFactory.create(this.store, acceptedValuesByField);

    // Loop over the scenarios
    for (int i = 0; i < scenariosArray.length; i++) {
      String scenario = (String) this.store.dictionaryProvider.dictionaryMap
              .get(Datastore.SCENARIO_FIELD)
              .read(scenariosArray[i]);

      ImmutableColumnVector[] columns = new ImmutableColumnVector[pointSize];
      for (int pointIndex = 0; pointIndex < columns.length; pointIndex++) {
        if (pointIndex != scenarioIndex) {
          columns[pointIndex] = this.store.getColumn(scenario, pointNames.get(pointIndex));
        }
      }

      List<Aggregator> aggregators = aggregatorsByScenario.get(scenario);
      IntIterable rowIterable = rowIterableProvider.apply(scenario);

      int scenarioDic = i;
      rowIterable.forEach(row -> {
        int[] point = new int[pointSize];
        for (int pointIndex = 0; pointIndex < pointSize; pointIndex++) {
          // When j == scenarioIndex, the value is already set from the pattern. See above.
          if (pointIndex != scenarioIndex) {
            point[pointIndex] = columns[pointIndex].getInt(row);
          } else {
            point[pointIndex] = scenariosArray[scenarioDic];
          }
        }

        int destinationRow = pointDictionary.map(point);
        // And then aggregate
        boolean check = false; // to do it only once
        for (Aggregator aggregator : aggregators) {
          if (!check) {
            aggregator.getDestination().ensureCapacity(destinationRow);
          }

          aggregator.aggregate(row, destinationRow);
        }
      });
    }

    return new PointListAggregateResult(
            pointDictionary,
            pointNames,
            pointNames.stream().map(pointName -> this.store.dictionaryProvider.get(pointName)).toList(),
            aggregates);
  }

  protected Map<String, MutableIntSet> computeAcceptedValues(Query query) {
    Map<String, MutableIntSet> acceptedValuesByField = new HashMap<>();
    query.coordinates.forEach((field, values) -> {
      if (field.equals(Datastore.SCENARIO_FIELD)) {
        return;
      }

      if (values == null) {
        // Wildcard, all values are accepted
      } else {
        Dictionary<Object> dictionary = this.store.dictionaryProvider.get(field);
        for (String value : values) {
          int position = dictionary.getPosition(value);
          if (position >= 0) {
            acceptedValuesByField.computeIfAbsent(field, key -> new IntHashSet()).add(position);
          }
        }
      }
    });
    return acceptedValuesByField;
  }

  protected Map<String, List<Aggregator>> computeAggregators(Query query, IntList queriedScenarios, List<ColumnVector> aggregates) {
    Map<String, List<Aggregator>> aggregatorsByScenario = new HashMap<>();
    queriedScenarios.forEachWithIndex((s, index) -> {
      String scenario = (String) this.store.dictionaryProvider.get(Datastore.SCENARIO_FIELD).read(s);
      List<Aggregator> aggregators = new ArrayList<>();
      if (index == 0) {
        query.measures.forEach(measure -> {
          if (measure instanceof AggregatedMeasure agg) {
            Aggregator aggregator = AggregatorFactory.create(
                    this.store.allocator,
                    this.store.getColumn(scenario, agg.field),
                    agg.aggregationFunction,
                    agg.alias());
            aggregators.add(aggregator);
            aggregates.add(aggregator.getDestination());
          } else {
            throw new RuntimeException("Not implemented yet");
          }
        });
      } else {
        // Here, we take the destination column created earlier.
        for (int i = 0; i < query.measures.size(); i++) {
          AggregatedMeasure agg = (AggregatedMeasure) query.measures.get(i);
          Aggregator aggregator = AggregatorFactory.create(
                  this.store.getColumn(scenario, agg.field),
                  aggregates.get(i),
                  agg.aggregationFunction);
          aggregators.add(aggregator);
        }
      }
      aggregatorsByScenario.put(scenario, aggregators);
    });
    return aggregatorsByScenario;
  }

  protected IntList computeQueriedScenarios(Query query) {
    List<String> values;
    if (query.coordinates.containsKey(Datastore.SCENARIO_FIELD)) {
      // This condition handles wildcard coordinates.
      values = query.coordinates.get(Datastore.SCENARIO_FIELD);
      if (values == null) { // Wildcard
        values = new ArrayList<>(this.store.vectorByFieldByScenario.keySet());
      }
    } else {
      values = Collections.singletonList(Datastore.MAIN_SCENARIO_NAME);
    }

    MutableIntList scenarios = new IntArrayList();
    Dictionary<Object> dictionary = this.store.dictionaryProvider.get(Datastore.SCENARIO_FIELD);
    for (String value : values) {
      int position = dictionary.getPosition(value);
      if (position >= 0) {
        scenarios.add(position);
      }
    }

    return scenarios;
  }
}

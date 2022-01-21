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
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A different version compatible with scenario.
 */
public class ArrowQueryEngineScenario {

  protected final ArrawDatastore store;

  public ArrowQueryEngineScenario(ArrawDatastore store) {
    this.store = store;
  }

  public PointListAggregateResult execute(Query query) {
    Map<String, MutableIntSet> acceptedValuesByField = new LinkedHashMap<>();

    int scenarioIndex = new ArrayList<>(query.coordinates.keySet()).indexOf(Datastore.SCENARIO_FIELD);
    MutableIntSet scenarios = new IntHashSet();
    query.coordinates.forEach((field, values) -> {
      if (values == null) {
        // wildcard, all values are accepted
        if (field.equals(Datastore.SCENARIO_FIELD)) {
          scenarios.add(-1); // means all.
        }
      } else {
        for (String value : values) {
          Dictionary<Object> dictionary = this.store.dictionaryProvider.get(field);
          int position = dictionary.getPosition(value);
          if (position >= 0) {
            if (field.equals(Datastore.SCENARIO_FIELD)) {
              scenarios.add(position);
            }
            acceptedValuesByField.computeIfAbsent(field, key -> new IntHashSet()).add(position);
          }
        }
      }
    });

    IntList queriedScenarios = getQueriedScenarios(scenarios);
    Map<String, List<Aggregator>> aggregatorsByScenario = new HashMap<>();
    List<ColumnVector> aggregates = new ArrayList<>();
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

    RoaringBitmap matchRows;
    if (acceptedValuesByField.isEmpty()) {
      // All lines are accepted
      matchRows = null;
    } else {
      // Take the first field that will be used as reference to build the first version of matching rows. Currently,
      // the same field (first one) is chosen, but we can imagine having a proper algorithm to choose it and try to
      // reduce the number of bit set into the map to accelerate any subsequent operations.
      throw new RuntimeException("not implemented");
    }

    int pointSize = query.coordinates.size();
    PointDictionary pointDictionary = new PointDictionary(pointSize);
    List<String> pointNames = FastList.newList(query.coordinates.keySet());

    int[][] patterns = createPointListPattern(scenarioIndex, pointSize, scenarios);

    // Loop over patterns
    for (int i = 0; i < patterns.length; i++) {
      int[] pattern = patterns[i];
      String currentScenario;
      if (scenarioIndex >= 0) {
        currentScenario = (String) this.store.dictionaryProvider.dictionaryMap.get(Datastore.SCENARIO_FIELD).read(pattern[scenarioIndex]);
      } else {
        currentScenario = Datastore.MAIN_SCENARIO_NAME;
      }

      // FIXME do the other way. Loop over the rows and for each row loop over the patterns => much more efficient
      // since we hope to hit same read a lot of time

      // Loop over the matching rows.
      for (int row = 0; row < this.store.rowCount; row++) {
        int[] buffer = new int[pointSize];
        System.arraycopy(pattern, 0, buffer, 0, buffer.length); // FIXME could be optimize to detect if pattern is "empty" and avoid this array copy

        // Fill the buffer
        for (int j = 0; j < pointSize; j++) {
          // When j == scenarioIndex, the value is already set from the pattern. See above.
          if (j != scenarioIndex) {
            // FIXME do not recreate it each time
            buffer[j] = this.store.getColumn(currentScenario, pointNames.get(j)).getInt(row);
          }
        }

        int destinationRow = pointDictionary.map(buffer);
        List<Aggregator> aggregators = aggregatorsByScenario.get(currentScenario);
        // And then aggregate
        boolean check = false; // to do it only once
        for (Aggregator aggregator : aggregators) {
          if (!check){
            aggregator.getDestination().ensureCapacity(destinationRow);
          }

          aggregator.aggregate(row, destinationRow);
        }
      }
    }

//    if (matchRows != null) {
//      matchRows.forEach(rowAggregator); // FIXME
//    }

    return new PointListAggregateResult(
            pointDictionary,
            pointNames,
            pointNames.stream().map(pointName -> this.store.dictionaryProvider.get(pointName)).collect(Collectors.toList()),
            aggregates);
  }

  private IntList getQueriedScenarios(MutableIntSet scenarios) {
    List<String> existingScenarios = new ArrayList<>(this.store.vectorByFieldByScenario.keySet());
    int[] arrayOfScenarios = scenarios.toArray();
    MutableIntList queriedScenarios;
    if (arrayOfScenarios.length == 0) {
      queriedScenarios = new IntArrayList(1);
      int position = this.store.dictionaryProvider.get(Datastore.SCENARIO_FIELD).getPosition(Datastore.MAIN_SCENARIO_NAME);
      queriedScenarios.add(position);
    } else if (arrayOfScenarios.length == 1) {
      int v = arrayOfScenarios[0];
      if (v < 0) {
        queriedScenarios = new IntArrayList(existingScenarios.size());
        for (int i = 0; i < existingScenarios.size(); i++) {
          int position = this.store.dictionaryProvider.get(Datastore.SCENARIO_FIELD).getPosition(existingScenarios.get(i));
          queriedScenarios.add(position);
        }
      } else {
        queriedScenarios = new IntArrayList(1);
        queriedScenarios.add(v);
      }
    } else {
      // a subset of scenario is querying
      queriedScenarios = new IntArrayList(arrayOfScenarios.length);
      for (int i = 0; i < arrayOfScenarios.length; i++) {
        queriedScenarios.add(arrayOfScenarios[i]);
      }
    }
    return queriedScenarios;
  }

  private int[][] createPointListPattern(int scenarioIndex, int pointSize, MutableIntSet scenarios) {
    int[][] pattern;
    List<String> existingScenarios = new ArrayList<>(this.store.vectorByFieldByScenario.keySet());
    int[] arrayOfScenarios = scenarios.toArray();
    if (arrayOfScenarios.length == 0) {
      // not even querying. default on base and nothing to do
      pattern = new int[1][pointSize];
    } else if (arrayOfScenarios.length == 1) {
      int v = arrayOfScenarios[0];
      if (v < 0) {
        pattern = new int[existingScenarios.size()][pointSize];
        for (int i = 0; i < existingScenarios.size(); i++) {
          pattern[i] = new int[pointSize];
          int position = this.store.dictionaryProvider.get(Datastore.SCENARIO_FIELD).getPosition(existingScenarios.get(i));
          pattern[i][scenarioIndex] = position;
        }
      } else {
        pattern = new int[1][pointSize];
        pattern[0][scenarioIndex] = v;
      }
    } else {
      // a subset of scenario is querying
      pattern = new int[arrayOfScenarios.length][pointSize];
      for (int i = 0; i < arrayOfScenarios.length; i++) {
        pattern[i] = new int[pointSize];
        pattern[i][scenarioIndex] = arrayOfScenarios[i];
      }
    }
    return pattern;
  }
}

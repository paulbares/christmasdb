package me.paulbares.arrow;

import me.paulbares.Datastore;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * TODO This needs to be unit tested. In particular the {@link #createInitialIterator()} part...
 */
public class BitmapRowIterableProvider implements RowIterableProvider {

  protected final Map<String, MutableIntSet> acceptedValuesByField;

  protected final ArrawDatastore store;

  protected final RoaringBitmap initialIterator;

  protected final List<String> fieldsWitSim = new ArrayList<>();

  public BitmapRowIterableProvider(
          ArrawDatastore store,
          Map<String, MutableIntSet> acceptedValuesByField) {
    if (acceptedValuesByField.containsKey(Datastore.SCENARIO_FIELD) && acceptedValuesByField.isEmpty()) {
      // The scenarios accepted values should be handled differently. This is a bug...
      throw new IllegalStateException("Not expected " + acceptedValuesByField);
    }
    this.store = store;
    this.acceptedValuesByField = acceptedValuesByField;
    this.initialIterator = createInitialIterator();
  }

  @Override
  public IntIterable get(String scenario) {
    RoaringBitmap bitmap = this.initialIterator;
    if (!this.fieldsWitSim.isEmpty()) {
      // Clone it because will be modified in-place
      applyConditions(bitmap.clone(), this.fieldsWitSim, scenario);
    }

    return new RoaringBitmapIntIterableAdapter(bitmap);
  }

  protected RoaringBitmap createInitialIterator() {
    // Keep only the fields that are not simulated
    List<String> fieldsWithoutSim = new ArrayList<>();
    for (String field : this.acceptedValuesByField.keySet()) {
      long c = this.store.vectorByFieldByScenario.entrySet()
              .stream().flatMap(e -> e.getValue().entrySet().stream())
              .filter(e -> e.getKey().equals(field))
              .count();
      if (c > 1) {
        this.fieldsWitSim.add(field);
      } else {
        fieldsWithoutSim.add(field);

      }
    }

    // Lexical sort to have a deterministic order
    Collections.sort(fieldsWithoutSim);
    Collections.sort(this.fieldsWitSim);

    List<ColumnVector> res = new ArrayList<>();
    fieldsWithoutSim.forEach(field -> res.add(this.store.vectorByFieldByScenario.get(Datastore.MAIN_SCENARIO_NAME).get(field)));

    String firstField = fieldsWithoutSim.remove(0);
    RoaringBitmap bitmap = initializeBitmap(
            this.acceptedValuesByField.get(firstField),
            this.store.getColumn(Datastore.MAIN_SCENARIO_NAME, firstField));
    applyConditions(bitmap, fieldsWithoutSim, Datastore.MAIN_SCENARIO_NAME);

    return bitmap;
  }

  protected void applyConditions(RoaringBitmap bitmap, List<String> fields, String scenario) {
    for (String field : fields) {
      RoaringBitmap tmp = new RoaringBitmap();
      MutableIntSet values = this.acceptedValuesByField.get(field);
      ImmutableColumnVector column = this.store.getColumn(scenario, field);
      bitmap.forEach((org.roaringbitmap.IntConsumer) row -> {
        if (values.contains(column.getInt(row))) {
          tmp.add(row);
        }
      });
      bitmap.and(tmp);
    }
  }

  /**
   * Creates the first {@link RoaringBitmap} to use.
   */
  protected RoaringBitmap initializeBitmap(IntSet acceptedValues, ImmutableColumnVector vector) {
    RoaringBitmap matchRows = new RoaringBitmap();
    for (int row = 0; row < this.store.rowCount; row++) {
      if (acceptedValues.contains(vector.getInt(row))) {
        matchRows.add(row);
      }
    }
    return matchRows;
  }
}

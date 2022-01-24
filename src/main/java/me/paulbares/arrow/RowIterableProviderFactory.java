package me.paulbares.arrow;

import org.eclipse.collections.api.set.primitive.MutableIntSet;

import java.util.Map;

public class RowIterableProviderFactory {

  public static RowIterableProvider create(
          ArrawDatastore store,
          Map<String, MutableIntSet> acceptedValuesByField) {
    if (acceptedValuesByField.isEmpty()) {
      return s -> new RangeIntIterable(0, store.rowCount);
    } else {
      return new BitmapRowIterableProvider(store, acceptedValuesByField);
    }
  }
}

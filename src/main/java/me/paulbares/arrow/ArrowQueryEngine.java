package me.paulbares.arrow;

import me.paulbares.dictionary.Dictionary;
import me.paulbares.query.Query;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrowQueryEngine {

  protected final ArrawDatastore store;

  public ArrowQueryEngine(ArrawDatastore store) {
    this.store = store;
  }

  public void execute(Query query) {
    int[] pattern = new int[query.coordinates.size()];
//    Arrays.fill(pattern, -1);

//    int[] indices = new int[1];
//    MutableIntList l = new IntArrayList();
    Map<Field, MutableIntSet> acceptedValuesByField = new HashMap<>();
    query.coordinates.forEach((field, values) -> {
      if (values == null) {
        // wildcard
      } else {
        for (String value : values) {
          Field f = Schema.findField(this.store.fields, field);
          Dictionary<Object> dictionary = this.store.dictionaryMap.get(f);
          int position = dictionary.getPosition(value);
          if (position >= 0) {
            acceptedValuesByField.computeIfAbsent(f, key -> new IntHashSet()).add(position);
          }
        }
      }
    });

    RoaringBitmap matchRows = null;

    // START handle simple condition
    if (!acceptedValuesByField.isEmpty()) {
      List<Field> fields = new ArrayList<>(acceptedValuesByField.keySet());
      FieldVector[][] vectors = new FieldVector[fields.size()][];
      for (int i = 0; i < fields.size(); i++) {
//        vectors[i] = this.store.fieldVectorsMap.get(fields.get(i));
      }

      IntSet values = acceptedValuesByField.get(fields.get(0));
      // Init the bitmap for the first time
      IntHolder intHolder = new IntHolder();
      for (int row = 0; row < this.store.rowCount; row++) {
        if (matchRows == null) {
          matchRows = new RoaringBitmap();
        }

//        int bucket = row >> this.store.logSize;
//        FieldReader reader = vectors[0][bucket].getReader();
//        int offset = row & this.store.sizeMinus1;

//        reader.setPosition(offset);
//        reader.read(intHolder);
        if (values.contains(intHolder.value)) {
          matchRows.add(row);
        }
      }

//      for (int i = 1; i < chunks.length; i++) {
//        IntSet v = acceptedValuesByField.get(fields.get(i));
//        RoaringBitmap tmp = new RoaringBitmap();
//        // Iterate over the bitmap
//        final int j = i;
//        matchRows.forEach((org.roaringbitmap.IntConsumer) row -> {
//          if (v.contains(chunks[j].readInt(row))) {
//            tmp.add(row);
//          }
//        });
//        matchRows.and(tmp);
//      }
    }
    // END handle simple condition
  }
}

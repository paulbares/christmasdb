package me.paulbares.arrow;

import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

record RoaringBitmapIntIterableAdapter(RoaringBitmap roaringBitmap) implements IntIterable {

  @Override
  public void forEach(IntProcedure procedure) {
    this.roaringBitmap.forEach((IntConsumer) r -> procedure.accept(r));
  }
}

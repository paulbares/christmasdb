package me.paulbares.arrow;

import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;

record RangeIntIterable(int start, int end) implements IntIterable {

  @Override
  public void forEach(IntProcedure procedure) {
    for (int i = this.start; i < this.end; i++) {
      procedure.accept(i);
    }
  }
}

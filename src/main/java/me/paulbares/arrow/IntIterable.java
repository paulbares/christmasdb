package me.paulbares.arrow;

import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;

interface IntIterable {
  void forEach(IntProcedure procedure);
}

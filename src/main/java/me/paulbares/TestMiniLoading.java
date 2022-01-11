package me.paulbares;

import org.apache.arrow.algorithm.dictionary.DictionaryBuilder;
import org.apache.arrow.algorithm.dictionary.HashTableBasedDictionaryBuilder;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;

public class TestMiniLoading {

  /**
   * Set arrow.enable_unsafe_memory_access to avoid bound checks
   * @param args
   */
  public static void main(String[] args) {
    RootAllocator allocator = new RootAllocator();
    String[] dataPojo = new String[]{"aa", "bb", "cc", "bb", "aa", "ee"};

    VarCharVector dicValues = new VarCharVector("dic values", allocator);
    DictionaryBuilder<VarCharVector> dicBuilder = new HashTableBasedDictionaryBuilder<>(dicValues);

    FieldVector data = new VarCharVector("data", allocator);
//    data.getReader().setPosition(2);
//    data.getReader().readBoolean();
//    int bufferSize = 4;

    // Batch 1
//    buffer.allocateNew(bufferSize);
    for (int i = 0; i < dataPojo.length; i++) {
//      if (i % bufferSize == 0 && i != 0) {
//        buffer.setValueCount(bufferSize);
//        dicBuilder.addValues(buffer);
//        buffer.clear();
//      }
//      dicBuilder.addValues(buffer);
//      data.set(i, dataPojo[i].getBytes());
    }

    System.out.println("DIC " + dicBuilder.getDictionary());

//    data.getDataBuffer().get
    data.getDataBuffer().getLong(1);
    data.getDataBuffer().getInt(1);

    // TODO not a good idea, Take a look to VectorSchemaRoot and what can be done with it.
//    ValueVector encode = DictionaryEncoder.encode(vector, new Dictionary(zob.getDictionary(),
//            new DictionaryEncoding(1L, false, /*indexType=*/null)));
  }
}

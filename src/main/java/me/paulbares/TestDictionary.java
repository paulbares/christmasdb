package me.paulbares;

import org.apache.arrow.algorithm.dictionary.DictionaryBuilder;
import org.apache.arrow.algorithm.dictionary.HashTableBasedDictionaryBuilder;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestDictionary {

  public static void main(String[] args) throws Exception {
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

    RootAllocator allocator = new RootAllocator();
    final VarCharVector dictVector = new VarCharVector("dict", allocator);
    dictVector.allocateNewSafe();
    dictVector.setSafe(0, "aa" .getBytes());
    dictVector.setSafe(1, "bb" .getBytes());
    dictVector.setSafe(2, "cc" .getBytes());
    dictVector.setValueCount(3);

    Dictionary dictionary = new Dictionary(dictVector, new DictionaryEncoding(1L, false, /*indexType=*/null));
    provider.put(dictionary);

    // create vector and encode it
    final VarCharVector vector = new VarCharVector("vector", allocator);
    vector.allocateNewSafe();
    vector.setSafe(0, "bb" .getBytes());
    vector.setSafe(1, "bb" .getBytes());
    vector.setSafe(2, "cc" .getBytes());
    vector.setSafe(3, "aa" .getBytes());
    vector.setValueCount(4);

    // get the encoded vector
    IntVector encodedVector = (IntVector) DictionaryEncoder.encode(vector, dictionary);

    {
      final VarCharVector toStoreValues = new VarCharVector("vector", allocator);
      DictionaryBuilder<VarCharVector> zob = new HashTableBasedDictionaryBuilder<>(toStoreValues);

      final VarCharVector batch = new VarCharVector("vector", allocator);
      int batchSize = 3;
      batch.allocateNewSafe();
      batch.setSafe(0, "bb" .getBytes());
      batch.setSafe(1, "bb" .getBytes());
      batch.setSafe(2, "cc" .getBytes());
      batch.setValueCount(3);
      zob.addValues(batch);

      batch.clear();
      batch.setSafe(0, "ee" .getBytes());
      batch.setSafe(1, "cc" .getBytes());
      batch.setSafe(2, "aa" .getBytes());
      batch.setValueCount(3);
      zob.addValues(batch);

      ValueVector encode = DictionaryEncoder.encode(vector, new Dictionary(zob.getDictionary(),
              new DictionaryEncoding(1L, false, /*indexType=*/null)));
      System.out.println(encode);
    }

    // create VectorSchemaRoot
    List<Field> fields = Arrays.asList(encodedVector.getField());
    List<FieldVector> vectors = Arrays.asList(encodedVector);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

    // write data
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ArrowStreamWriter writer = new ArrowStreamWriter(root, provider, Channels.newChannel(out));
    writer.start();
    writer.writeBatch();
    writer.end();

    // read data
    try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
      reader.loadNextBatch();
      VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
      // get the encoded vector
      IntVector intVector = (IntVector) readRoot.getVector(0);

      // get dictionaries and decode the vector
      Map<Long, Dictionary> dictionaryMap = reader.getDictionaryVectors();
      long dictionaryId = intVector.getField().getDictionary().getId();
      VarCharVector varCharVector =
              (VarCharVector) DictionaryEncoder.decode(intVector, dictionaryMap.get(dictionaryId));
      System.out.println(varCharVector);


      ///////

//      VarCharVector dictionary = new VarCharVector("", allocator);
//
//      dictionary.allocateNew();

    }
  }
}

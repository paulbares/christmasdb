package me.paulbares;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class TestVectorSchemaRoot {

  public static void main(String[] args) throws Exception {
    RootAllocator allocator = new RootAllocator();
    BitVector bitVector = new BitVector("boolean", allocator);
    VarCharVector varCharVector = new VarCharVector("varchar", allocator);
    bitVector.allocateNew();
    varCharVector.allocateNew();
    for (int i = 0; i < 10; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(10);
    varCharVector.setValueCount(10);

    List<org.apache.arrow.vector.types.pojo.Field> fields = Arrays.asList(bitVector.getField(),
            varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ArrowStreamWriter writer = new ArrowStreamWriter(root, /*DictionaryProvider=*/null, Channels.newChannel(out));

    writer.start();
    writer.writeBatch();
    writer.end();


    try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
      Schema schema = reader.getVectorSchemaRoot().getSchema();
      for (int i = 0; i < 5; i++) {
        // This will be loaded with new values on every call to loadNextBatch
        VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
        reader.loadNextBatch();

        FieldVector vector = readBatch.getVector(1);
        System.out.println(vector);
//    ... do something with readBatch
      }

    }
  }
}
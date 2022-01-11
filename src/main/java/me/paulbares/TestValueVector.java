package me.paulbares;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * --add-opens=java.base/java.nio=ALL-UNNAMED
 */
public class TestValueVector {

  public static void main(String[] args) {
    RootAllocator allocator = new RootAllocator();
    ArrowType type = Types.MinorType.SMALLINT.getType();
    FieldType fieldType = new FieldType(false, type, new DictionaryEncoding(1, false, null));
    IntVector prix = new IntVector("prix", fieldType, allocator);

    prix.allocateNew(10);
    prix.set(1, 5);
    prix.set(0, 3);
    prix.setSafe(0, 3);

    System.out.println(prix.get(1));
    prix.setValueCount(10);
    prix.set(0, 3);

    FieldVector prix2 = fieldType.createNewSingleVector("prix2", allocator, null); // Test with other type
    Dictionary dictionary = new Dictionary(prix2, new DictionaryEncoding(1, false, null));
    DictionaryEncoder dictionaryEncoder = new DictionaryEncoder(dictionary, allocator);

    VarCharVector vector = new VarCharVector("vector", allocator);
    vector.allocateNew();
    vector.set(0, "zero".getBytes());
    vector.set(1, "one".getBytes());
    vector.set(2, "two".getBytes());
    vector.setValueCount(3);
    System.out.println(new String(vector.get(2)));

    ValueVector encode = dictionaryEncoder.encode(vector);
//    encode.getReader().read
    System.out.println();
    //    encode.get
//    vector.setSafe(5, "five".getBytes());
//    vector.setSafe(6, "six".getBytes());
//    System.out.println(new String(vector.get(9)));

  }
}

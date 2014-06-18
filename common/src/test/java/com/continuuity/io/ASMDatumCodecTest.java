package com.continuuity.io;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.continuuity.internal.io.ASMDatumWriterFactory;
import com.continuuity.internal.io.ASMFieldAccessorFactory;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ASMDatumCodecTest {

  private static final ASMDatumWriterFactory DATUM_WRITER_FACTORY
    = new ASMDatumWriterFactory(new ASMFieldAccessorFactory());

  /**
   *
   */
  public static enum TestEnum {
    VALUE1, VALUE2, VALUE3, VALUE4
  }

  private <T> Schema getSchema(TypeToken<T> type) throws UnsupportedTypeException {
    return new ReflectionSchemaGenerator().generate(type.getType());
  }

  private <T> DatumWriter<T> getWriter(TypeToken<T> type) throws UnsupportedTypeException {
    Schema schema =  getSchema(type);
    return DATUM_WRITER_FACTORY.create(type, schema);
  }

  @Test
  public void testShort() throws UnsupportedTypeException, IOException {
    TypeToken<Short> type = new TypeToken<Short>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);
    DatumWriter<Short> writer = getWriter(type);
    writer.encode((short) 3000, new BinaryEncoder(os));

    ReflectionDatumReader<Short> reader = new ReflectionDatumReader<Short>(getSchema(type), type);
    short value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals((short) 3000, value);
  }

  @Test
  public void testInt() throws UnsupportedTypeException, IOException {
    TypeToken<Integer> type = new TypeToken<Integer>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);
    DatumWriter<Integer> writer = getWriter(type);
    writer.encode(12234234, new BinaryEncoder(os));

    ReflectionDatumReader<Integer> reader = new ReflectionDatumReader<Integer>(getSchema(type), type);
    int value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals(12234234, value);
  }

  @Test
  public void testDouble() throws UnsupportedTypeException, IOException {
    TypeToken<Double> type = new TypeToken<Double>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);
    DatumWriter<Double> writer = getWriter(type);
    writer.encode(3.14d, new BinaryEncoder(os));

    ReflectionDatumReader<Double> reader = new ReflectionDatumReader<Double>(getSchema(type), type);
    double value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals(3.14d, value, 0.000001d);
  }

  @Test
  public void testString() throws UnsupportedTypeException, IOException {
    TypeToken<String> type = new TypeToken<String>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);
    DatumWriter<String> writer = getWriter(type);
    writer.encode("Testing message", new BinaryEncoder(os));

    ReflectionDatumReader<String> reader = new ReflectionDatumReader<String>(getSchema(type), type);
    String value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals("Testing message", value);
  }

  @Test
  public void testUUID() throws UnsupportedTypeException, IOException {
    TypeToken<UUID> type = new TypeToken<UUID>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);
    DatumWriter<UUID> writer = getWriter(type);

    UUID uuid = UUID.randomUUID();
    writer.encode(uuid, new BinaryEncoder(os));

    ReflectionDatumReader<UUID> reader = new ReflectionDatumReader<UUID>(getSchema(type), type);
    UUID value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals(uuid, value);
  }

  @Test
  public void testEnum() throws UnsupportedTypeException, IOException {
    TypeToken<TestEnum> type = new TypeToken<TestEnum>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);
    DatumWriter<TestEnum> writer = getWriter(type);
    BinaryEncoder encoder = new BinaryEncoder(os);
    writer.encode(TestEnum.VALUE1, encoder);
    writer.encode(TestEnum.VALUE4, encoder);
    writer.encode(TestEnum.VALUE3, encoder);

    ReflectionDatumReader<TestEnum> reader = new ReflectionDatumReader<TestEnum>(getSchema(type), type);

    TestEnum value = reader.read(new BinaryDecoder(is), getSchema(type));
    Assert.assertEquals(TestEnum.VALUE1, value);

    value = reader.read(new BinaryDecoder(is), getSchema(type));
    Assert.assertEquals(TestEnum.VALUE4, value);

    value = reader.read(new BinaryDecoder(is), getSchema(type));
    Assert.assertEquals(TestEnum.VALUE3, value);
  }

  @Test
  public void testPrimitiveArray() throws IOException, UnsupportedTypeException {
    TypeToken<int[]> type = new TypeToken<int[]>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    int[] writeValue = {1, 2, 3, 4, -5, -6, -7, -8};
    DatumWriter<int[]> writer = getWriter(type);
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<int[]> reader = new ReflectionDatumReader<int[]>(getSchema(type), type);

    int[] value = reader.read(new BinaryDecoder(is), getSchema(type));
    Assert.assertArrayEquals(writeValue, value);
  }

  @Test
  public void testReferenceArray() throws IOException, UnsupportedTypeException {
    TypeToken<String[]> type = new TypeToken<String[]>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    String[] writeValue = new String[] {"1", "2", null, "3"};
    DatumWriter<String[]> writer = getWriter(type);
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<String[]> reader = new ReflectionDatumReader<String[]>(getSchema(type), type);

    String[] value = reader.read(new BinaryDecoder(is), getSchema(type));
    Assert.assertArrayEquals(writeValue, value);
  }

  @Test
  public void testList() throws IOException, UnsupportedTypeException {
    TypeToken<List<Long>> type = new TypeToken<List<Long>>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    List<Long> writeValue = ImmutableList.of(1L, 10L, 100L, 1000L);
    DatumWriter<List<Long>> writer = getWriter(type);
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<List<Long>> reader = new ReflectionDatumReader<List<Long>>(getSchema(type), type);

    List<Long> value = reader.read(new BinaryDecoder(is), getSchema(type));
    Assert.assertEquals(writeValue, value);
  }

  @Test
  public void testMap() throws IOException, UnsupportedTypeException {
    TypeToken<Map<String, List<String>>> type = new TypeToken<Map<String, List<String>>>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    DatumWriter<Map<String, List<String>>> writer = getWriter(type);
    ImmutableMap<String, List<String>> map = ImmutableMap.<String, List<String>>of("k1", Lists.newArrayList("v1"),
                                                                "k2", Lists.newArrayList("v2", null));
    writer.encode(map, new BinaryEncoder(os));

    ReflectionDatumReader<Map<String, List<String>>> reader =
      new ReflectionDatumReader<Map<String, List<String>>>(getSchema(type), type);

    Assert.assertEquals(map, reader.read(new BinaryDecoder(is), getSchema(type)));
  }

  @Test
  public void testURI() throws IOException, UnsupportedTypeException {
    TypeToken<List<URI>> type = new TypeToken<List<URI>>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    DatumWriter<List<URI>> writer = getWriter(type);
    List<URI> writeValue = ImmutableList.of(URI.create("http://www.continuuity.com"));
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<List<URI>> reader = new ReflectionDatumReader<List<URI>>(getSchema(type), type);
    Assert.assertEquals(writeValue, reader.read(new BinaryDecoder(is), getSchema(type)));
  }

  private static class Record {
    private int i;
    private String s;
    private List<String> list;
    private TestEnum e;

    public Record(int i, String s, List<String> list, TestEnum e) {
      this.i = i;
      this.s = s;
      this.list = list;
      this.e = e;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Record record = (Record) o;

      return i == record.i && e == record.e && list.equals(record.list) && s.equals(record.s);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(i, s, list, e);
    }
  }

  @Test
  public void testRecord() throws IOException, UnsupportedTypeException {
    TypeToken<Record> type = new TypeToken<Record>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    DatumWriter<Record> writer = getWriter(type);
    Record writeValue = new Record(10, "testing", ImmutableList.of("a", "b", "c"), TestEnum.VALUE2);
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<Record> reader = new ReflectionDatumReader<Record>(getSchema(type), type);
    Record value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals(writeValue, value);
  }

  @Test
  public void testRecordContainer() throws IOException, UnsupportedTypeException {
    TypeToken<List<Record>> type = new TypeToken<List<Record>>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    DatumWriter<List<Record>> writer = getWriter(type);
    List<Record> writeValue = ImmutableList.of(
                                  new Record(10, "testing", ImmutableList.of("a", "b", "c"), TestEnum.VALUE2));
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<List<Record>> reader = new ReflectionDatumReader<List<Record>>(getSchema(type), type);
    List<Record> value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals(writeValue, value);
  }

  @Test
  public void testRecordArray() throws IOException, UnsupportedTypeException {
    TypeToken<Record[][]> type = new TypeToken<Record[][]>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    DatumWriter<Record[][]> writer = getWriter(type);
    Record[][] writeValue = new Record[][] {{ new Record(10, "testing",
                                                         ImmutableList.of("a", "b", "c"), TestEnum.VALUE2)}};
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<Record[][]> reader = new ReflectionDatumReader<Record[][]>(getSchema(type), type);
    Record[][] value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertArrayEquals(writeValue, value);
  }

  /**
   *
   */
  public static final class Node {
    public int data;
    public Node left;
    public Node right;

    public Node(int data, Node left, Node right) {
      this.data = data;
      this.left = left;
      this.right = right;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Node node = (Node) o;

      return data == node.data
               && (left  != null ? left.equals(node.left) : node.left == null)
               && (right != null ? right.equals(node.right) : node.right == null);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(data, left, right);
    }
  }

  @Test
  public void testTree() throws IOException, UnsupportedTypeException {
    TypeToken<Node> type = new TypeToken<Node>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    DatumWriter<Node> writer = getWriter(type);
    Node root = new Node(1, new Node(2, null, new Node(3, null, null)), new Node(4, new Node(5, null, null), null));
    writer.encode(root, new BinaryEncoder(os));

    ReflectionDatumReader<Node> reader = new ReflectionDatumReader<Node>(getSchema(type), type);
    Node value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals(root, value);
  }

  @Test
  public void testStreamEvent() throws IOException, UnsupportedTypeException {
    TypeToken<StreamEvent> type = new TypeToken<StreamEvent>() { };
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    DatumWriter<StreamEvent> writer = getWriter(type);
    StreamEvent event = new DefaultStreamEvent(ImmutableMap.of("key", "value"),
                                               ByteBuffer.wrap("Testing message".getBytes(Charsets.UTF_8)));
    writer.encode(event, new BinaryEncoder(os));

    ReflectionDatumReader<StreamEvent> reader = new ReflectionDatumReader<StreamEvent>(getSchema(type), type);
    StreamEvent value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals(event.getHeaders(), value.getHeaders());
    Assert.assertEquals(event.getBody(), value.getBody());
  }

  @Ignore
  @Test
  public void testSpeed() throws UnsupportedTypeException, IOException {
    TypeToken<Node> type = new TypeToken<Node>() { };
    ByteArrayOutputStream os = new ByteArrayOutputStream(1024);

    long startTime;
    long endTime;

    Node writeValue = new Node(1, new Node(2, null, new Node(3, null, null)),
                               new Node(4, new Node(5, null, null), null));

    DatumWriter<Node> writer = getWriter(type);
    startTime = System.nanoTime();
    for (int i = 0; i < 100000; i++) {
      os.reset();
      writer.encode(writeValue, new BinaryEncoder(os));
    }
    endTime = System.nanoTime();
    System.out.println("Time spent: " + TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));

    ReflectionDatumWriter<Node> datumWriter = new ReflectionDatumWriter<Node>(getSchema(type));
    startTime = System.nanoTime();
    for (int i = 0; i < 100000; i++) {
      os.reset();
      datumWriter.encode(writeValue, new BinaryEncoder(os));
    }
    endTime = System.nanoTime();
    System.out.println("Time spent: " + TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));

    writer = getWriter(type);
    startTime = System.nanoTime();
    for (int i = 0; i < 100000; i++) {
      os.reset();
      writer.encode(writeValue, new BinaryEncoder(os));
    }
    endTime = System.nanoTime();
    System.out.println("Time spent: " + TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));

    datumWriter = new ReflectionDatumWriter<Node>(getSchema(type));
    startTime = System.nanoTime();
    for (int i = 0; i < 100000; i++) {
      os.reset();
      datumWriter.encode(writeValue, new BinaryEncoder(os));
    }
    endTime = System.nanoTime();
    System.out.println("Time spent: " + TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
  }
}

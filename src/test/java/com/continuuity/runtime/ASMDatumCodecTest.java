package com.continuuity.runtime;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.internal.api.io.Schema;
import com.continuuity.internal.api.io.UnsupportedTypeException;
import com.continuuity.internal.app.runtime.flow.ASMDatumWriterFactory;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureVisitor;
import org.objectweb.asm.signature.SignatureWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ASMDatumCodecTest {
//
//  public static final class MyOutputEmitter extends AbstractOutputEmitter<String[]> {
//
//    public MyOutputEmitter() {
//      super(null, null, null, null);
//    }
//
//    @Override
//    protected void encodeData(String[] data, OutputStream os) throws IOException {
//      //To change body of implemented methods use File | Settings | File Templates.
//    }
//  }
//

  public static final class MyDatumWriter implements DatumWriter<Map<String, List<String>>> {

    private final Schema schema;

    public MyDatumWriter(Schema schema) {
      this.schema = schema;
    }

    @Override
    public void encode(Map<String, List<String>> data, Encoder encoder) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }
  }

  private static final ASMDatumWriterFactory DATUM_WRITER_FACTORY = new ASMDatumWriterFactory();

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
  public void testInt() throws UnsupportedTypeException, IOException {
    TypeToken<Integer> type = new TypeToken<Integer>() {};
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
    TypeToken<Double> type = new TypeToken<Double>() {};
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
    TypeToken<String> type = new TypeToken<String>() {};
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);
    DatumWriter<String> writer = getWriter(type);
    writer.encode("Testing message", new BinaryEncoder(os));

    ReflectionDatumReader<String> reader = new ReflectionDatumReader<String>(getSchema(type), type);
    String value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals("Testing message", value);
  }

  @Test
  public void testEnum() throws UnsupportedTypeException, IOException {
    TypeToken<TestEnum> type = new TypeToken<TestEnum>() {};
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
    TypeToken<int[]> type = new TypeToken<int[]>() {};
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    System.out.println(getSchema(type));

    int[] writeValue = {1, 2, 3, 4, -5, -6, -7, -8};
    DatumWriter<int[]> writer = getWriter(type);
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<int[]> reader = new ReflectionDatumReader<int[]>(getSchema(type), type);

    int[] value = reader.read(new BinaryDecoder(is), getSchema(type));
    Assert.assertArrayEquals(writeValue, value);
  }

  @Test
  public void testMap() throws IOException, UnsupportedTypeException {
    TypeToken<Map<String, List<String>>> type = new TypeToken<Map<String, List<String>>>() {};
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

  public static class Record {
    public int i;
    public String s;
    public List<String> list;
    public TestEnum e;

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
    TypeToken<Record> type = new TypeToken<Record>() {};
    PipedOutputStream os = new PipedOutputStream();
    PipedInputStream is = new PipedInputStream(os);

    DatumWriter<Record> writer = getWriter(type);
    Record writeValue = new Record(10, "testing", ImmutableList.of("a", "b", "c"), TestEnum.VALUE2);
    writer.encode(writeValue, new BinaryEncoder(os));

    ReflectionDatumReader<Record> reader = new ReflectionDatumReader<Record>(getSchema(type), type);
    Record value = reader.read(new BinaryDecoder(is), getSchema(type));

    Assert.assertEquals(writeValue, value);
  }

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

      if (data != node.data) {
        return false;
      }
      if (left != null ? !left.equals(node.left) : node.left != null) {
        return false;
      }
      if (right != null ? !right.equals(node.right) : node.right != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(data, left, right);
    }
  }

  @Test
  public void testTree() throws IOException, UnsupportedTypeException {
    TypeToken<Node> type = new TypeToken<Node>() {};
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
    TypeToken<StreamEvent> type = new TypeToken<StreamEvent>() {};
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

  @Test
  public void testSpeed() throws UnsupportedTypeException, IOException {
    TypeToken<Node> type = new TypeToken<Node>() {};
    ByteArrayOutputStream os = new ByteArrayOutputStream(1024);

    long startTime;
    long endTime;

    Node writeValue = new Node(1, new Node(2, null, new Node(3, null, null)), new Node(4, new Node(5, null, null), null));

    DatumWriter<Node> writer = getWriter(type);
    startTime = System.nanoTime();
    for (int i = 0; i < 100000; i++) {
      os.reset();
      writer.encode(writeValue, new BinaryEncoder(os));
    }
    endTime = System.nanoTime();
    System.out.println("Time spent: " + TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));

    ReflectionDatumWriter datumWriter = new ReflectionDatumWriter(getSchema(type));
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

    datumWriter = new ReflectionDatumWriter(getSchema(type));
    startTime = System.nanoTime();
    for (int i = 0; i < 100000; i++) {
      os.reset();
      datumWriter.encode(writeValue, new BinaryEncoder(os));
    }
    endTime = System.nanoTime();
    System.out.println("Time spent: " + TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
  }

  @Test
  public void testReadClass() throws IOException {
    System.out.println(Type.getDescriptor(Schema.class));

    ClassReader reader = new ClassReader(MyDatumWriter.class.getName());
    reader.accept(new ClassVisitor(Opcodes.ASM4) {

      @Override
      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        System.out.println(name);
        System.out.println(signature);
        System.out.println(superName);
        System.out.println(Arrays.toString(interfaces));
        System.out.println();
        super.visit(version, access, name, signature, superName, interfaces);
      }

      @Override
      public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
        System.out.println(name);
        System.out.println(desc);
        System.out.println(signature);
        System.out.println(value);
        System.out.println();
        return super.visitField(access, name, desc, signature, value);
      }

      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        System.out.println(name);
        System.out.println(desc);
        System.out.println(signature);
        System.out.println(Arrays.toString(exceptions));
        System.out.println();
        return super.visitMethod(access, name, desc, signature, exceptions);
      }
    }, 0);
  }

  private String getMethodSignature(TypeToken<?> objectType, java.lang.reflect.Method method) {
    SignatureWriter signWriter = new SignatureWriter();
    for (java.lang.reflect.Type type : method.getGenericParameterTypes()) {
      TypeToken<?> argType = objectType.resolveType(type);
      SignatureVisitor sv = signWriter.visitParameterType();
      visitSignature(argType, sv, true);
    }

    TypeToken<?> returnType = objectType.resolveType(method.getGenericReturnType());
    visitSignature(returnType, signWriter.visitReturnType(), false);

    return signWriter.toString();
  }

  private void visitSignature(TypeToken<?> type, SignatureVisitor visitor, boolean visitEnd) {
    Class<?> rawType = type.getRawType();
    if (rawType.isPrimitive()) {
      visitor.visitBaseType(Type.getType(rawType).toString().charAt(0));
    } else {
      visitor.visitClassType(Type.getType(rawType).getInternalName());
    }

    java.lang.reflect.Type visitType = type.getType();
    if (visitType instanceof ParameterizedType) {
      for (java.lang.reflect.Type argType : ((ParameterizedType) visitType).getActualTypeArguments()) {
        visitSignature(TypeToken.of(argType), visitor.visitTypeArgument(SignatureVisitor.INSTANCEOF), true);
      }
    }
    if (visitEnd) {
      visitor.visitEnd();
    }
  }
}

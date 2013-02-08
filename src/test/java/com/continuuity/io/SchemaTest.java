package com.continuuity.io;

import com.google.common.reflect.TypeToken;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class SchemaTest {

  public static final class Node {
    private int data;
    private List<Node> children;
  }

  public static class Parent<T> {
    private T data;
    private ByteBuffer buffer;
  }

  public static class Child<T> extends Parent<Map<String, T>> {
    private int height;
    private Node rootNode;
    private State state;

    private static enum State {
      OK, ERROR
    }
  }

  @Test
  public void testGenerateSchema() throws UnsupportedTypeException {
    SchemaGenerator gen = new ReflectionSchemaGenerator();
    Schema schema = gen.generate((new TypeToken<Child<String>>(){}).getType());

    // TODO: Do assertion
    System.out.println(schema);
  }
}

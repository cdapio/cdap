/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.io;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.SchemaTypeAdapter;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Test for schema generation.
 */
public class SchemaTest {

  /**
   * Test node.
   */
  public final class Node {
    private int data;
    private List<Node> children;
  }

  /**
   * Test parent.
   * @param <T> Parameter
   */
  public class Parent<T> {
    private T data;
    private ByteBuffer buffer;
  }

  /**
   * Test child.
   * @param <T> Paramter.
   */
  public class Child<T> extends Parent<Map<String, T>> {
    private int height;
    private Node rootNode;
    private State state;
  }

  /**
   * Test enum.
   */
  public enum State {
    OK, ERROR
  }


  @Test
  public void testGenerateSchema() throws UnsupportedTypeException {
    Schema schema = (new ReflectionSchemaGenerator()).generate((new TypeToken<Child<Node>>() { }).getType());

    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();

    Assert.assertEquals(schema, gson.fromJson(gson.toJson(schema), Schema.class));
  }

  /**
   * Testing more node.
   */
  public final class Node2 {
    private int data;
    private List<Node2> children;
  }

  @Test
  public void testSchemaHash() throws UnsupportedTypeException {
    Schema s1 = new ReflectionSchemaGenerator().generate(Node.class);
    Schema s2 = new ReflectionSchemaGenerator().generate(Node2.class);

    Assert.assertEquals(s1.getSchemaHash(), s2.getSchemaHash());
    Assert.assertEquals(s1, s2);

    Schema schema = (new ReflectionSchemaGenerator()).generate((new TypeToken<Child<Node>>() { }).getType());
    Assert.assertNotEquals(s1.getSchemaHash(), schema.getSchemaHash());
  }

  /**
   * Yet more node.
   */
  public final class Node3 {
    private long data;
    private String tag;
    private List<Node3> children;
  }

  /**
   * More and more node.
   */
  public final class Node4 {
    private String data;
  }

  @Test
  public void testCompatible() throws UnsupportedTypeException {
    Schema s1 = new ReflectionSchemaGenerator().generate(Node.class);
    Schema s2 = new ReflectionSchemaGenerator().generate(Node3.class);
    Schema s3 = new ReflectionSchemaGenerator().generate(Node4.class);

    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(s1.isCompatible(s2));
    Assert.assertFalse(s2.isCompatible(s1));

    Assert.assertTrue(s2.isCompatible(s3));
  }

  @Test
  public void testPrimitiveArray() throws UnsupportedTypeException {
    Schema schema = new ReflectionSchemaGenerator().generate(int[].class);
    Assert.assertEquals(Schema.arrayOf(Schema.of(Schema.Type.INT)), schema);
  }

  @Test
  public void testParse() throws IOException, UnsupportedTypeException {
    Schema schema = new ReflectionSchemaGenerator().generate(Node.class);
    Assert.assertEquals(schema, Schema.parse(schema.toString()));
  }
}

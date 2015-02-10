/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.Partitioning.FieldType;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

/**
 *  Tests for the partitioning and its field types.
 */
public class PartitioningTest {

  @Test(expected = IllegalStateException.class)
  public void testBuilderEmpty() {
    Partitioning.builder().build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderDuplicate() {
    Partitioning.builder()
      .addField("name", FieldType.STRING)
      .addIntField("age")
      .addStringField("name")
      .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderNullName() {
    Partitioning.builder().addField(null, FieldType.STRING).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderNullType() {
    Partitioning.builder().addField("x", null).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderEmptyName() {
    Partitioning.builder().addStringField("").build();
  }

  @Test
  public void testBuilderGetters() {
    Partitioning partitioning = Partitioning.builder()
      .addField("a", FieldType.STRING)
      .addField("b", FieldType.INT)
      .addField("c", FieldType.LONG)
      .addStringField("d")
      .addIntField("e")
      .addLongField("f")
      .build();
    Assert.assertEquals(FieldType.STRING, partitioning.getFieldType("a"));
    Assert.assertEquals(FieldType.INT,    partitioning.getFieldType("b"));
    Assert.assertEquals(FieldType.LONG,   partitioning.getFieldType("c"));
    Assert.assertEquals(FieldType.STRING, partitioning.getFieldType("d"));
    Assert.assertEquals(FieldType.INT,    partitioning.getFieldType("e"));
    Assert.assertEquals(FieldType.LONG,   partitioning.getFieldType("f"));
    Assert.assertNull(partitioning.getFieldType("x"));
    Assert.assertEquals(partitioning.getFields().keySet(), ImmutableSet.of("a", "b", "c", "d", "e", "f"));
  }

  @Test
  public void testFieldOrder() {
    Partitioning partitioning = Partitioning.builder()
      .addIntField("1")
      .addLongField("2")
      .addStringField("3")
      .build();
    Iterator<Map.Entry<String, FieldType>> iterator = partitioning.getFields().entrySet().iterator();
    Assert.assertEquals("1", iterator.next().getKey());
    Assert.assertEquals("2", iterator.next().getKey());
    Assert.assertEquals("3", iterator.next().getKey());
    Assert.assertFalse(iterator.hasNext());

    // the previous order may have been preserved by chance. Now try the reverse order
    partitioning = Partitioning.builder()
      .addIntField("3")
      .addLongField("2")
      .addStringField("1")
      .build();
    iterator = partitioning.getFields().entrySet().iterator();
    Assert.assertEquals("3", iterator.next().getKey());
    Assert.assertEquals("2", iterator.next().getKey());
    Assert.assertEquals("1", iterator.next().getKey());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testFieldTypes() {
    testFieldType(FieldType.STRING, "", "a", "bb", "cde", "aa", "cdee");
    testFieldType(FieldType.INT, 0, -1, Integer.MIN_VALUE, -42, 1, Integer.MAX_VALUE, 4356728);
    testFieldType(FieldType.LONG, 0L, -1L, Long.MIN_VALUE, -42L, 1L, Long.MAX_VALUE, 4356728L);
  }

  // test that fromBytes/toBytes are inverse of each other, for each field type
  // also test that toBytes preserves the ordering of values
  private <T extends Comparable<T>>
  void testFieldType(FieldType type, T... values) {
    byte[][] byteValues = new byte[values.length][];
    int index = 0;
    for (Comparable value : values) {
      Assert.assertTrue(FieldTypes.validateType(value, type));
      byte[] byteValue = FieldTypes.toBytes(value, type);
      int determined = FieldTypes.determineLengthInBytes(byteValue, 0, type);
      Assert.assertEquals(byteValue.length, determined);
      Assert.assertEquals(value, FieldTypes.fromBytes(byteValue, 0, determined, type));
      Assert.assertEquals(value, type.parse(value.toString()));
      byteValues[index++] = byteValue;
    }
    for (int i = 0; i < values.length; i++) {
      for (int j = 0; j < values.length; j++) {
        Assert.assertEquals(Integer.signum(values[i].compareTo(values[j])),
                            Integer.signum(Bytes.compareTo(byteValues[i], byteValues[j])));
      }
    }
  }

}

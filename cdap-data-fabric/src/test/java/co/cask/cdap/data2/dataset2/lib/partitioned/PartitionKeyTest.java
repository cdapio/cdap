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

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.Partitioning;
import com.google.common.base.Function;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Tests for partition keys.
 */
public class PartitionKeyTest {

  @Test(expected = IllegalStateException.class)
  public void testBuilderEmpty() {
    PartitionKey.builder().build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderNullField() {
    PartitionKey.builder().addIntField(null, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderEmptyField() {
    PartitionKey.builder().addIntField("", 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderNullValue() {
    PartitionKey.builder().addStringField("x", null).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderDuplicateField() {
    PartitionKey.builder().addField("x", 10).addField("y", 10L).addField("x", 14).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderUnknownField() {
    PartitionKey.builder(
      Partitioning.builder().addIntField("x").addLongField("y").build())
      .addField("x", 10).addField("y", 10L).addField("z", 15).build();
  }

  @Test()
  @SuppressWarnings("ConstantConditions")
  public void testBuilderIllegalFieldValue() {
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addLongField("x", 1L);
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addField("x", 1L);
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addStringField("x", "a");
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addField("x", "a");
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addIntField("y", 1);
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addField("y", 1);
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addStringField("y", "a");
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addField("y", "a");
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addIntField("z", 1);
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addField("z", 1);
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addLongField("z", 1L);
      }
    });
    testIllegalFieldValue(new Function<PartitionKey.Builder, PartitionKey.Builder>() {
      @Nullable
      @Override
      public PartitionKey.Builder apply(@Nullable PartitionKey.Builder builder) {
        return builder.addField("z", 1L);
      }
    });
  }

  private void testIllegalFieldValue(Function<PartitionKey.Builder, PartitionKey.Builder> function) {
    PartitionKey.Builder builder = PartitionKey.builder(
      Partitioning.builder().addIntField("x").addLongField("y").addStringField("z").build());
    try {
      function.apply(builder);
      Assert.fail("builder should have thrown exception for invalid field type");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderMissingField() {
    PartitionKey.builder(
      Partitioning.builder().addIntField("x").addLongField("y").addStringField("z").build())
      .addField("x", 10).addField("y", 10L).build();
  }

  @Test
  public void testBuilderGetter() {
    PartitionKey key = PartitionKey.builder()
      .addField("a", "value")
      .addField("b", 1L)
      .addField("c", -17)
      .addField("d", true)
      .addIntField("e", 42)
      .addLongField("f", 15)
      .addStringField("g", "ghijk")
      .build();

    Assert.assertEquals("value", key.getField("a"));
    Assert.assertEquals(1L, key.getField("b"));
    Assert.assertEquals(-17, key.getField("c"));
    Assert.assertEquals(true, key.getField("d"));
    Assert.assertEquals(42, key.getField("e"));
    Assert.assertEquals(15L, key.getField("f"));
    Assert.assertEquals("ghijk", key.getField("g"));
  }

  @Test
  public void testEqualityHashCode() {
    PartitionKey key1 = PartitionKey.builder()
      .addField("a", "value")
      .addField("b", 1L)
      .addField("c", -17)
      .build();
    PartitionKey key2 = PartitionKey.builder()
      .addField("b", 1L)
      .addField("c", -17)
      .addField("a", "value")
      .build();
    Assert.assertEquals(key1, key2);
    Assert.assertEquals(key1.hashCode(), key2.hashCode());
  }
}

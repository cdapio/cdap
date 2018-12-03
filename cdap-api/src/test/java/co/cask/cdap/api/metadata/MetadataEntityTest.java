/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.api.metadata;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test for  {@link MetadataEntity}
 */
public class MetadataEntityTest {

  @Test
  public void testInvalidKeys() {
    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
        .appendAsType(MetadataEntity.DATASET, "ds").append(MetadataEntity.DATASET, "soemthing");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      MetadataEntity.builder().append("", "ns");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testImmutability() {
    MetadataEntity.Builder builder = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.DATASET, "ds");
    MetadataEntity original = builder.build();
    // add more to the builder
    MetadataEntity modified = builder.appendAsType("field", "f1").build();

    Assert.assertEquals(MetadataEntity.DATASET, original.getType());
    assertEquals(original.getKeys(), MetadataEntity.NAMESPACE, MetadataEntity.DATASET);
    assertEquals(original.getValues(), "ns", "ds");

    Assert.assertEquals("field", modified.getType());
    assertEquals(modified.getKeys(), MetadataEntity.NAMESPACE, MetadataEntity.DATASET, "field");
    assertEquals(modified.getValues(), "ns", "ds", "f1");
  }

  @Test
  public void testOf() {
    MetadataEntity metadataEntity = MetadataEntity.ofNamespace("ns");
    assertEquals(metadataEntity.getKeys(), MetadataEntity.NAMESPACE);
    assertEquals(metadataEntity.getValues(), "ns");

    metadataEntity = MetadataEntity.ofDataset("ns", "ds");
    assertEquals(metadataEntity.getKeys(), MetadataEntity.NAMESPACE, MetadataEntity.DATASET);
    assertEquals(metadataEntity.getValues(), "ns", "ds");

    metadataEntity = MetadataEntity.ofDataset("ds");
    assertEquals(metadataEntity.getKeys(), MetadataEntity.DATASET);
    assertEquals(metadataEntity.getValues(), "ds");
  }

  @Test
  public void testBuilder() {
    // empty builder should fail
    try {
      MetadataEntity.builder().build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    // test that builder builds metadata entity correctly
    MetadataEntity metadataEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.DATASET, "myDataset").build();
    assertEquals(metadataEntity.getKeys(), MetadataEntity.NAMESPACE, MetadataEntity.DATASET);
    assertEquals(metadataEntity.getValues(), "ns", "myDataset");
  }

  @Test
  public void testValidateHierarchy() {
    // test which tests building of various system metadata entity to verify the hierarchy

    // test dataset
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.DATASET, "myDataset").build();
    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").append("b", "c")
        .appendAsType(MetadataEntity.DATASET, "myDataset").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      MetadataEntity.builder().append("unknown", "a").appendAsType(MetadataEntity.DATASET, "ds").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    // should pass since
    MetadataEntity.builder().appendAsType(MetadataEntity.DATASET, "myDataset").build();

    // should not fail
    MetadataEntity metadataEntity = MetadataEntity.builder().append(MetadataEntity.DATASET, "myDataset").build();
    Assert.assertEquals(MetadataEntity.DATASET, metadataEntity.getType());

    // test custom types
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").append(MetadataEntity.DATASET, "ds")
      .appendAsType("field", "myField").build();

    // test application
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.APPLICATION, "myApp").build();
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.APPLICATION, "myApp").append(MetadataEntity.VERSION, "1").build();

    // test artifact
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.ARTIFACT, "myArt").append(MetadataEntity.VERSION, "1").build();

    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
        .appendAsType(MetadataEntity.ARTIFACT, "myArt").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    // test program
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.TYPE, "worflow").appendAsType(MetadataEntity.PROGRAM, "someProg").build();
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.VERSION, "1").append(MetadataEntity.TYPE, "worflow")
      .appendAsType(MetadataEntity.PROGRAM, "someProg").build();
    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").append(MetadataEntity.TYPE, "worflow")
        .appendAsType(MetadataEntity.PROGRAM, "someProg").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").append(MetadataEntity.VERSION, "1")
        .append(MetadataEntity.TYPE, "worflow").appendAsType(MetadataEntity.PROGRAM, "someProg").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    // test schedule
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
      .appendAsType(MetadataEntity.SCHEDULE, "sched").build();
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.VERSION, "1").appendAsType(MetadataEntity.SCHEDULE, "sched").build();
    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
        .appendAsType(MetadataEntity.SCHEDULE, "sched").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").append(MetadataEntity.VERSION, "1")
        .appendAsType(MetadataEntity.SCHEDULE, "sched").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    // test program run
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.TYPE, "worflow").append(MetadataEntity.PROGRAM, "someProg")
      .appendAsType(MetadataEntity.PROGRAM_RUN, "r1").build();
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.VERSION, "1").append(MetadataEntity.TYPE, "worflow")
      .append(MetadataEntity.PROGRAM, "someProg").appendAsType(MetadataEntity.PROGRAM_RUN, "r1").build();
    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
        .append(MetadataEntity.TYPE, "worflow").appendAsType(MetadataEntity.PROGRAM_RUN, "r1").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    // test flowlet
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.FLOW, "flow").appendAsType(MetadataEntity.FLOWLET, "ft").build();
    MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").appendAsType(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.VERSION, "1").append(MetadataEntity.FLOW, "flow")
      .appendAsType(MetadataEntity.FLOWLET, "ft").build();
    try {
      MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
        .append(MetadataEntity.FLOW, "flow").appendAsType(MetadataEntity.FLOWLET, "ft").build();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testGetTill() {
    MetadataEntity metadataEntity = MetadataEntity.builder(MetadataEntity.ofNamespace("ns"))
      .appendAsType(MetadataEntity.APPLICATION, "myApp").append(MetadataEntity.TYPE, "worflow")
      .appendAsType(MetadataEntity.PROGRAM, "someProg").build();

    List<MetadataEntity.KeyValue> till = metadataEntity.head(MetadataEntity.APPLICATION);
    Assert.assertEquals(2, till.size());
    Assert.assertEquals(new MetadataEntity.KeyValue(MetadataEntity.NAMESPACE, "ns"), till.get(0));
    Assert.assertEquals(new MetadataEntity.KeyValue(MetadataEntity.APPLICATION, "myApp"), till.get(1));

    try {
      metadataEntity.head("nonExisting");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void assertEquals(Iterable<String> iterator, String... expected) {
    List<String> actual = new ArrayList<>();
    iterator.forEach(actual::add);
    Assert.assertEquals(Arrays.asList(expected), actual);
  }
}

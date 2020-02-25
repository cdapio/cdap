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

package io.cdap.cdap.etl.lineage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class StageOperationsValidatorTest {

  @Test
  public void testInvalidInputs() {
    FieldReadOperation read = new FieldReadOperation("read", "reading data", EndPoint.of("default", "file"),
                                                     "offset", "body");

    StageOperationsValidator.Builder builder = new StageOperationsValidator.Builder(Collections.singletonList(read));
    builder.addStageOutputs(Arrays.asList("offset", "body"));
    StageOperationsValidator stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();
    Assert.assertNull(stageOperationsValidator.getStageInvalids());

    FieldTransformOperation transform = new FieldTransformOperation("parse", "parsing data",
                                                                    Collections.singletonList("body"),
                                                                    Arrays.asList("name", "address", "zip"));
    builder = new StageOperationsValidator.Builder(Collections.singletonList(transform));
    builder.addStageInputs(Arrays.asList("offset", "body"));
    builder.addStageOutputs(Arrays.asList("name", "address", "zip"));
    stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();
    Assert.assertNull(stageOperationsValidator.getStageInvalids());

    FieldWriteOperation write = new FieldWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                        "name", "address", "zip");
    builder = new StageOperationsValidator.Builder(Collections.singletonList(write));
    builder.addStageInputs(Arrays.asList("address", "zip"));
    stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();
    Assert.assertNotNull(stageOperationsValidator.getStageInvalids());

    InvalidFieldOperations invalidFieldOperations = stageOperationsValidator.getStageInvalids();
    Assert.assertEquals(1, invalidFieldOperations.getInvalidInputs().size());
    Map<String, List<String>> invalidInputs = new HashMap<>();
    invalidInputs.put("name", Collections.singletonList("write"));
    Assert.assertEquals(invalidInputs, invalidFieldOperations.getInvalidInputs());
    Assert.assertEquals(0, invalidFieldOperations.getInvalidOutputs().size());

    // name is provided by output of the operation previous to write
    List<FieldOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new FieldTransformOperation("name_lookup", "generating name",
                                                       Collections.singletonList("address"), "name"));
    pipelineOperations.add(new FieldWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                   "name", "address", "zip"));

    builder = new StageOperationsValidator.Builder(pipelineOperations);
    builder.addStageInputs(Arrays.asList("address", "zip"));
    stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();
    Assert.assertNull(stageOperationsValidator.getStageInvalids());
  }

  @Test
  public void testUnusedValidOutputs() {
    // the read operation has [offset, body] as output fields, the output schema only has [body], it
    // should still be valid
    FieldOperation read = new FieldReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                                 "body");
    StageOperationsValidator.Builder builder = new StageOperationsValidator.Builder(Collections.singletonList(read));
    builder.addStageOutputs(Collections.singleton("body"));
    StageOperationsValidator stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();

    Assert.assertNull(stageOperationsValidator.getStageInvalids());

    // the transform has [body] -> [name, address, zip], the output schema has [name, address], it is still valid
    // since the field is generated by the input schema fields
    FieldOperation transform = new FieldTransformOperation("parse", "parsing data",
                                                           Collections.singletonList("body"),
                                                           Arrays.asList("name", "address", "zip"));
    builder = new StageOperationsValidator.Builder(Collections.singletonList(transform));
    builder.addStageInputs(Collections.singleton("body"));
    builder.addStageOutputs(Arrays.asList("name", "address"));
    stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();
    Assert.assertNull(stageOperationsValidator.getStageInvalids());

    // simulate joiner, the operations are [d1.k1, d2.k2] -> [k1, k2], [d1.val1] -> [val1], [d2.val2] -> [val2],
    // the input schema are [d1.k1, d2.k2, d1.val1, d1.val2], output schemas are [val1, val2], this is still valid
    // since both d1.k1, d2.k2 are in the input schema though k1, k2 are not in output schema.
    FieldTransformOperation transform1 = new FieldTransformOperation("transform1", "description1",
                                                                     ImmutableList.of("d1.k1", "d2.k2"),
                                                                     ImmutableList.of("k1", "k2"));
    FieldTransformOperation transform2 = new FieldTransformOperation("transform2", "description2",
                                                                     ImmutableList.of("d1.val1"),
                                                                     ImmutableList.of("val1"));
    FieldTransformOperation transform3 = new FieldTransformOperation("transform3", "description3",
                                                                      ImmutableList.of("d2.val2"),
                                                                      ImmutableList.of("val2"));
    builder = new StageOperationsValidator.Builder(ImmutableList.of(transform1, transform2, transform3));
    builder.addStageInputs(ImmutableList.of("d1.k1", "d2.k2", "d1.val1", "d2.val2"));
    builder.addStageOutputs(ImmutableList.of("val1", "val2"));
    StageOperationsValidator validator = builder.build();
    validator.validate();
    Assert.assertNull(validator.getStageInvalids());
  }

  @Test
  public void testUnusedInvalidInputs() {
    // for write operation, all the inputs are written to the endpoint
    FieldOperation write = new FieldWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                   "name", "address", "zip");

    StageOperationsValidator.Builder builder = new StageOperationsValidator.Builder(Collections.singletonList(write));
    builder.addStageInputs(Arrays.asList("name", "address"));
    StageOperationsValidator stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();
    InvalidFieldOperations expected =
      new InvalidFieldOperations(ImmutableMap.of("zip", Collections.singletonList("write")), Collections.emptyMap());
    Assert.assertEquals(expected, stageOperationsValidator.getStageInvalids());

    // the input field is not in a non-existing field, so invalid
    FieldTransformOperation transform = new FieldTransformOperation("transform", "description",
                                                                     ImmutableList.of("nonexist"),
                                                                     ImmutableList.of("val2"));
    builder = new StageOperationsValidator.Builder(Collections.singletonList(transform));
    builder.addStageInputs(Collections.singletonList("exist"));
    builder.addStageOutputs(Collections.singletonList("val2"));
    StageOperationsValidator validator = builder.build();
    validator.validate();
    expected = new InvalidFieldOperations(Collections.singletonMap("nonexist", Collections.singletonList("transform")),
                                          Collections.emptyMap());
    InvalidFieldOperations stageInvalids = validator.getStageInvalids();
    Assert.assertEquals(expected, stageInvalids);
  }

  @Test
  public void testRedundantOutputs() {
    List<FieldOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new FieldTransformOperation("redundant_parse", "parsing data",
                                                       Collections.singletonList("body"), "name"));
    pipelineOperations.add(new FieldTransformOperation("parse", "parsing data", Collections.singletonList("body"),
                                                       Arrays.asList("name", "address", "zip")));

    StageOperationsValidator.Builder builder = new StageOperationsValidator.Builder(pipelineOperations);
    builder.addStageInputs(Arrays.asList("offset", "body"));
    builder.addStageOutputs(Arrays.asList("name", "address", "zip"));
    StageOperationsValidator stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();

    Map<String, List<String>> expected = ImmutableMap.of("name", Collections.singletonList("redundant_parse"));

    Assert.assertEquals(expected, stageOperationsValidator.getRedundantOutputs());
  }

  @Test
  public void testRedundantOutputUsedAsInput() {
    List<FieldOperation> pipelineOperations = new ArrayList<>();
    pipelineOperations.add(new FieldTransformOperation("redundant_parse1", "parsing data",
                                                       Collections.singletonList("body"), "name"));
    pipelineOperations.add(new FieldTransformOperation("redundant_parse2", "parsing data",
                                                       Collections.singletonList("body"), "name"));
    pipelineOperations.add(new FieldTransformOperation("non_redundant_parse", "parsing data",
                                                       Collections.singletonList("body"), "name"));
    pipelineOperations.add(new FieldTransformOperation("parse", "parsing data", Arrays.asList("body", "name"),
                                                       Arrays.asList("name", "address", "zip")));

    StageOperationsValidator.Builder builder = new StageOperationsValidator.Builder(pipelineOperations);
    builder.addStageInputs(Arrays.asList("offset", "body"));
    builder.addStageOutputs(Arrays.asList("name", "address", "zip"));
    StageOperationsValidator stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();

    Map<String, List<String>> expected =
      ImmutableMap.of("name", Arrays.asList("redundant_parse1", "redundant_parse2"));

    Assert.assertEquals(expected, stageOperationsValidator.getRedundantOutputs());
  }
}

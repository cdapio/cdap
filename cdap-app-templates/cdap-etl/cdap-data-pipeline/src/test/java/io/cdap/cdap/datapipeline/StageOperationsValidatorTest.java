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

package io.cdap.cdap.datapipeline;

import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;
import com.google.common.collect.ImmutableMap;
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
  public void testUnusedOutputs() {
    FieldOperation read = new FieldReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                                 "body");
    StageOperationsValidator.Builder builder = new StageOperationsValidator.Builder(Collections.singletonList(read));
    builder.addStageOutputs(Collections.singleton("body"));
    StageOperationsValidator stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();

    InvalidFieldOperations expected = new InvalidFieldOperations(Collections.emptyMap(),
                                                                 ImmutableMap.of("offset",
                                                                                 Collections.singletonList("read")));
    Assert.assertEquals(expected, stageOperationsValidator.getStageInvalids());

    FieldOperation transform = new FieldTransformOperation("parse", "parsing data",
                                                           Collections.singletonList("body"),
                                                           Arrays.asList("name", "address", "zip"));
    builder = new StageOperationsValidator.Builder(Collections.singletonList(transform));
    builder.addStageInputs(Collections.singleton("body"));
    builder.addStageOutputs(Arrays.asList("name", "address"));
    stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();
    expected = new InvalidFieldOperations(Collections.emptyMap(),
                                          ImmutableMap.of("zip", Collections.singletonList("parse")));
    Assert.assertEquals(expected, stageOperationsValidator.getStageInvalids());

    FieldOperation write = new FieldWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                   "name", "address", "zip");

    builder = new StageOperationsValidator.Builder(Collections.singletonList(write));
    builder.addStageInputs(Arrays.asList("name", "address"));
    stageOperationsValidator = builder.build();
    stageOperationsValidator.validate();
    expected = new InvalidFieldOperations(ImmutableMap.of("zip", Collections.singletonList("write")),
                                          Collections.emptyMap());
    Assert.assertEquals(expected, stageOperationsValidator.getStageInvalids());
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

    InvalidFieldOperations expected =
      new InvalidFieldOperations(Collections.emptyMap(),
                                 ImmutableMap.of("name", Collections.singletonList("redundant_parse")));

    Assert.assertEquals(expected, stageOperationsValidator.getStageInvalids());
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

    InvalidFieldOperations expected =
      new InvalidFieldOperations(Collections.emptyMap(),
                                 ImmutableMap.of("name", Arrays.asList("redundant_parse1", "redundant_parse2")));

    Assert.assertEquals(expected, stageOperationsValidator.getStageInvalids());
  }
}

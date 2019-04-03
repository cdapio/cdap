/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Tests for {@link MultipleInputs}.
 */
public class MultipleInputsTest {

  @Test
  public void testConfigurations() throws IOException, ClassNotFoundException {
    Job job = Job.getInstance();

    String inputName1 = "inputName1";
    String inputFormatClass1 = TextInputFormat.class.getName();
    Map<String, String> inputFormatConfigs1 = ImmutableMap.of("key1", "val1", "key2", "val2");
    MultipleInputs.addInput(job, inputName1, inputFormatClass1, inputFormatConfigs1, job.getMapperClass());

    Map<String, MultipleInputs.MapperInput> map = MultipleInputs.getInputMap(job.getConfiguration());

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(inputName1, Iterables.getOnlyElement(map.keySet()));
    Assert.assertEquals(inputFormatClass1, Iterables.getOnlyElement(map.values()).getInputFormatClassName());
    Assert.assertEquals(inputFormatConfigs1, Iterables.getOnlyElement(map.values()).getInputFormatConfiguration());
    Assert.assertEquals(job.getMapperClass().getName(), Iterables.getOnlyElement(map.values()).getMapperClassName());

    Assert.assertEquals(MultiInputFormat.class, job.getInputFormatClass());

    // now, test with two inputs in the configuration
    String inputName2 = "inputName2";
    String inputFormatClass2 = TextInputFormat.class.getName();
    Map<String, String> inputFormatConfigs2 = ImmutableMap.of("some_key1", "some_val1", "some_key2", "some_val2");
    MultipleInputs.addInput(job, inputName2, inputFormatClass2, inputFormatConfigs2, CustomMapper.class);

    map = MultipleInputs.getInputMap(job.getConfiguration());

    Assert.assertEquals(2, map.size());

    MultipleInputs.MapperInput mapperInput1 = map.get(inputName1);
    Assert.assertEquals(inputFormatClass1, mapperInput1.getInputFormatClassName());
    Assert.assertEquals(inputFormatConfigs1, mapperInput1.getInputFormatConfiguration());
    Assert.assertEquals(job.getMapperClass().getName(), mapperInput1.getMapperClassName());

    MultipleInputs.MapperInput mapperInput2 = map.get(inputName2);
    Assert.assertEquals(inputFormatClass2, mapperInput2.getInputFormatClassName());
    Assert.assertEquals(inputFormatConfigs2, mapperInput2.getInputFormatConfiguration());
    Assert.assertEquals(CustomMapper.class, job.getConfiguration().getClassByName(mapperInput2.getMapperClassName()));
  }

  public static final class CustomMapper extends Mapper {

  }
}

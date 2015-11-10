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

package co.cask.cdap.internal.app.runtime.batch.dataset;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class MultipleOutputsTest {

  @Test
  public void testNamedConfigurations() throws IOException {
    Job job = Job.getInstance(new Configuration());

    Map<String, String> name1Config = ImmutableMap.of("key1", "value1",
                                                      "key2", "value2");
    Map<String, String> name2Config = ImmutableMap.of("name2key", "name2value");
    Map<String, String> emptyConfig = ImmutableMap.of();

    MultipleOutputs.setNamedConfigurations(job, "name1", name1Config);
    MultipleOutputs.setNamedConfigurations(job, "name2", name2Config);
    MultipleOutputs.setNamedConfigurations(job, "emptyConfig", emptyConfig);


    Assert.assertEquals(name1Config, MultipleOutputs.getNamedConfigurations(job, "name1"));
    Assert.assertEquals(name2Config, MultipleOutputs.getNamedConfigurations(job, "name2"));
    Assert.assertEquals(emptyConfig, MultipleOutputs.getNamedConfigurations(job, "emptyConfig"));
  }
}

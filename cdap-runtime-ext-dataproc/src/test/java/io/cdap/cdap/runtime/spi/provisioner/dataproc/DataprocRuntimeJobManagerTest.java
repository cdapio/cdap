/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocRuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.twill.api.LocalFile;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests for DataprocRuntimeJobManager.
 */
public class DataprocRuntimeJobManagerTest {

  private static RuntimeJobInfo runtimeJobInfo;

  @BeforeClass
  public static void setUp() {
    runtimeJobInfo = new RuntimeJobInfo() {
      private final ProgramRunInfo runInfo = new ProgramRunInfo.Builder()
        .setNamespace("namespace").setApplication("application").setVersion("1.0")
        .setProgramType("workflow").setProgram("program").setRun(UUID.randomUUID().toString()).build();
      @Override
      public Collection<? extends LocalFile> getLocalizeFiles() {
        return Collections.emptyList();
      }

      @Override
      public String getRuntimeJobClassname() {
        return DataprocRuntimeJobManager.getJobId(runInfo);
      }

      @Override
      public ProgramRunInfo getProgramRunInfo() {
        return runInfo;
      }

      @Override
      public Map<String, String> getJvmProperties() {
        return ImmutableMap.of("key", "val");
      }
    };
  }

  @Test
  public void jobNameTest() {
    ProgramRunInfo runInfo = new ProgramRunInfo.Builder()
      .setNamespace("namespace").setApplication("application").setVersion("1.0")
      .setProgramType("workflow").setProgram("program").setRun(UUID.randomUUID().toString()).build();
    String jobName = DataprocRuntimeJobManager.getJobId(runInfo);
    Assert.assertTrue(jobName.startsWith("namespace_application_program"));
  }

  @Test
  public void longJobNameTest() {
    ProgramRunInfo runInfo = new ProgramRunInfo.Builder()
      .setNamespace("namespace").setApplication("very_very_long_app_name_is_provided_this_should_be" +
                                                  "_trimed_so_that_correct_name_is_produced")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program").setRun(UUID.randomUUID().toString()).build();
    String jobName = DataprocRuntimeJobManager.getJobId(runInfo);
    Assert.assertTrue(jobName.startsWith("namespace_very_very_long_app_name_is_provided_this_should_be_tr_"));
    Assert.assertEquals(100, jobName.length());
  }

  @Test (expected = IllegalArgumentException.class)
  public void invalidJobNameTest() {
    ProgramRunInfo runInfo = new ProgramRunInfo.Builder()
      .setNamespace("namespace").setApplication("application$$$")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun(UUID.randomUUID().toString()).build();
    String jobName = DataprocRuntimeJobManager.getJobId(runInfo);
    Assert.assertTrue(jobName.startsWith("namespace_application_program"));
  }

  @Test
  public void getArgumentsTest() {
    List<String> arguments = DataprocRuntimeJobManager.getArguments(runtimeJobInfo, Collections.emptyList(),
                                                                    SparkCompat.SPARK3_2_12.getCompat());
    Assert.assertEquals(3, arguments.size());
    Assert.assertTrue(arguments.contains("--propkey=\"val\""));
    Assert.assertTrue(arguments.contains("--runtimeJobClass=" + runtimeJobInfo.getRuntimeJobClassname()));
    Assert.assertTrue(arguments.contains("--sparkCompat=" + SparkCompat.SPARK3_2_12.getCompat()));

  }

  @Test
  public void getPropertiesTest() {
    Map<String, String> properties = DataprocRuntimeJobManager.getProperties(runtimeJobInfo);
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    Assert.assertEquals(6, properties.size());
    Assert.assertEquals(runInfo.getNamespace(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_NAMESPACE));
    properties.put(runInfo.getApplication(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_APPLICATION));
    Assert.assertEquals(runInfo.getVersion(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_VERSION));
    Assert.assertEquals(runInfo.getProgramType(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_PROGRAM_TYPE));
    Assert.assertEquals(runInfo.getProgram(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_PROGRAM));
    Assert.assertEquals(runInfo.getRun(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_RUNID));
  }
}

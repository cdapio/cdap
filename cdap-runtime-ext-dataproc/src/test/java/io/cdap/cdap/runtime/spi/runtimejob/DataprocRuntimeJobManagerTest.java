/*
 *  Copyright © 2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.runtime.spi.runtimejob;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests for DataprocRuntimeJobManager.
 */
public class DataprocRuntimeJobManagerTest {

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
  public void testProfilerName() {
    Assert.assertEquals("cdap-abc", DataprocRuntimeJobManager.getProfilerServiceName("abc"));
    Assert.assertEquals("cdap-abc", DataprocRuntimeJobManager.getProfilerServiceName("a!@#$%^&b*()+[]/<>:\"'c`"));
    Assert.assertEquals("cdap-abc-0", DataprocRuntimeJobManager.getProfilerServiceName("abc-"));
    Assert.assertEquals("cdap-abc_0", DataprocRuntimeJobManager.getProfilerServiceName("abc_"));
    Assert.assertEquals("cdap-abc.0", DataprocRuntimeJobManager.getProfilerServiceName("abc."));

    StringBuilder expected = new StringBuilder("cdap-");
    StringBuilder longStr = new StringBuilder();
    for (int i = 0; i < 249; i++) {
      int c = i % 10;
      expected.append(c);
      longStr.append(c);
    }
    expected.append("0");
    longStr.append("0123456789...");
    Assert.assertEquals(expected.toString(), DataprocRuntimeJobManager.getProfilerServiceName(longStr.toString()));
  }
}

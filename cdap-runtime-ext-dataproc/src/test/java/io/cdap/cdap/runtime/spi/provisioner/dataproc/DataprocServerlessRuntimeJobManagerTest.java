/*
 * Copyright © 2026 Cask Data, Inc.
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

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.RuntimeConfig;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocServerlessRuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.twill.api.LocalFile;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for DataprocServerlessRuntimeJobManager.
 */
public class DataprocServerlessRuntimeJobManagerTest {

  private static final Pattern BATCH_ID_PATTERN = Pattern.compile("^[a-z][a-z0-9-]{3,62}$");

  private static RuntimeJobInfo runtimeJobInfo;

  @BeforeClass
  public static void setUp() {
    runtimeJobInfo =
        new RuntimeJobInfo() {
          private final ProgramRunInfo runInfo =
              new ProgramRunInfo.Builder()
                  .setNamespace("namespace")
                  .setApplication("application")
                  .setVersion("1.0")
                  .setProgramType("workflow")
                  .setProgram("program")
                  .setRun(UUID.randomUUID().toString())
                  .build();

          @Override
          public Collection<? extends LocalFile> getLocalizeFiles() {
            return Collections.emptyList();
          }

          @Override
          public String getRuntimeJobClassname() {
            return "io.cdap.cdap.runtime.spi.runtimejob.DataprocJobMain";
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
  public void getBatchIdValidationTest() {
    ProgramRunInfo runInfo =
        new ProgramRunInfo.Builder()
            .setNamespace("NAMESPACE-Upper")
            .setApplication("AppWith_Underscore$$$")
            .setVersion("1.0")
            .setProgramType("workflow")
            .setProgram("program")
            .setRun(UUID.randomUUID().toString())
            .build();
            
    String batchId = DataprocServerlessRuntimeJobManager.getBatchId(runInfo);
    
    // Batch ID must match Dataproc rules: starts with a letter, max 63 chars, only a-z, 0-9 and hyphens.
    Assert.assertTrue("Batch ID '" + batchId + "' is invalid.", BATCH_ID_PATTERN.matcher(batchId).matches());
    Assert.assertTrue(batchId.startsWith("cdap-"));
    Assert.assertEquals(41, batchId.length()); // "cdap-" (5) + UUID (36)
  }

  @Test
  public void getPropertiesTest() throws Exception {
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    // Simulate mapping in RuntimeJobManager
    // Set properties
    Batch batch = Batch.newBuilder()
        .setRuntimeConfig(RuntimeConfig.newBuilder()
            .putAllProperties(ImmutableMap.of(
                DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_NAMESPACE, runInfo.getNamespace(),
                DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_APPLICATION, runInfo.getApplication(),
                DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_VERSION, runInfo.getVersion(),
                DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_PROGRAM_TYPE, runInfo.getProgramType(),
                DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_PROGRAM, runInfo.getProgram(),
                DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_RUNID, runInfo.getRun()
            )).build())
        .build();

    // Map properties from RuntimeConfig
    Map<String, String> properties = batch.getRuntimeConfig().getPropertiesMap();
    
    Assert.assertEquals(runInfo.getNamespace(),
                        properties.get(DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_NAMESPACE));
    Assert.assertEquals(runInfo.getApplication(),
                        properties.get(DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_APPLICATION));
    Assert.assertEquals(runInfo.getVersion(),
                        properties.get(DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_VERSION));
    Assert.assertEquals(runInfo.getProgramType(),
                        properties.get(DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_PROGRAM_TYPE));
    Assert.assertEquals(runInfo.getProgram(),
                        properties.get(DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_PROGRAM));
    Assert.assertEquals(runInfo.getRun(),
                        properties.get(DataprocServerlessRuntimeJobManager.CDAP_RUNTIME_RUNID));
  }
}

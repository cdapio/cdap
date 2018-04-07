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

package co.cask.cdap.runtime.spi.provisioner.dataproc;

import co.cask.cdap.runtime.spi.provisioner.ProgramRun;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests for DataProc provisioner
 */
public class DataProcProvisionerTest {

  @Test
  public void testClusterName() {
    // test basic
    ProgramRun programRun = new ProgramRun("ns", "app", "program", UUID.randomUUID().toString());
    Assert.assertEquals("app-" + programRun.getRun(), DataProcProvisioner.getClusterName(programRun));

    // test lowercasing and stripping of invalid characters
    programRun = new ProgramRun("ns", "My@Appl!cation", "program", UUID.randomUUID().toString());
    Assert.assertEquals("myapplcation-" + programRun.getRun(), DataProcProvisioner.getClusterName(programRun));

    // test truncating long
    String longName = "abcdefghijklmnopqrstuvwxyz01234567890";
    programRun = new ProgramRun("ns", longName, "program", UUID.randomUUID().toString());
    Assert.assertEquals("abcdefghijklmnopq-" + programRun.getRun(),
                        DataProcProvisioner.getClusterName(programRun));
  }
}

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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Tests for Dataproc provisioner
 */
public class DataprocProvisionerTest {

  @Test
  public void testClusterName() {
    // test basic
    ProgramRun programRun = new ProgramRun("ns", "app", "program", UUID.randomUUID().toString());
    Assert.assertEquals("cdap-app-" + programRun.getRun(), DataprocProvisioner.getClusterName(programRun));

    // test lowercasing, stripping of invalid characters, and truncation
    programRun = new ProgramRun("ns", "My@Appl!cation", "program", UUID.randomUUID().toString());
    Assert.assertEquals("cdap-myapplcat-" + programRun.getRun(), DataprocProvisioner.getClusterName(programRun));
  }

  @Test
  public void testParseSingleLabel() {
    Map<String, String> expected = new HashMap<>();
    expected.put("key", "val");
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("key=val"));
  }

  @Test
  public void testParseMultipleLabels() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "v1");
    expected.put("k2", "v2");
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("k1=v1,k2=v2"));
  }

  @Test
  public void testParseLabelsIgnoresWhitespace() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "v1");
    expected.put("k2", "v2");
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels(" k1  =\tv1  ,\nk2 = v2  "));
  }

  @Test
  public void testParseLabelsWithoutVal() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "");
    expected.put("k2", "");
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("k1,k2="));
  }

  @Test
  public void testParseLabelsIgnoresInvalidKey() {
    Map<String, String> expected = new HashMap<>();
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("A"));
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("0"));
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("a.b"));
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("a^b"));
    StringBuilder longStr = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      longStr.append('a');
    }
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels(longStr.toString()));
  }

  @Test
  public void testParseLabelsIgnoresInvalidVal() {
    Map<String, String> expected = new HashMap<>();
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("a=A"));
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels("a=ab.c"));
    StringBuilder longStr = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      longStr.append('a');
    }
    Assert.assertEquals(expected, DataprocProvisioner.parseLabels(String.format("a=%s", longStr.toString())));
  }
}

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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;
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
    ProgramRunInfo programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace("ns")
      .setApplication("app")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun(UUID.randomUUID().toString())
      .build();
    Assert.assertEquals("cdap-app-" + programRunInfo.getRun(), DataprocProvisioner.getClusterName(programRunInfo));

    // test lowercasing, stripping of invalid characters, and truncation
    programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace("ns")
      .setApplication("My@Appl!cation")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun(UUID.randomUUID().toString())
      .build();
    Assert.assertEquals("cdap-myapplcat-" + programRunInfo.getRun(),
                        DataprocProvisioner.getClusterName(programRunInfo));
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

  @Test
  public void testDataprocConf() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("zone", "region1-a");
    props.put("network", "network");
    props.put("spark:spark.reducer.maxSizeInFlight", "100");
    props.put("hadoop-env:MAPREDUCE_CLASSPATH", "xyz");
    props.put("dataproc:am.primary_only", "true");

    DataprocConf conf = DataprocConf.fromProperties(props);

    Assert.assertEquals(conf.getProjectId(), "pid");
    Assert.assertEquals(conf.getRegion(), "region1");
    Assert.assertEquals(conf.getZone(), "region1-a");

    Map<String, String> dataprocProps = conf.getDataprocProperties();
    Assert.assertEquals(3, dataprocProps.size());

    Assert.assertEquals("100", dataprocProps.get("spark:spark.reducer.maxSizeInFlight"));
    Assert.assertEquals("xyz", dataprocProps.get("hadoop-env:MAPREDUCE_CLASSPATH"));
    Assert.assertEquals("true", dataprocProps.get("dataproc:am.primary_only"));
  }

  @Test
  public void testAutoZone() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("zone", "auto-detect");
    props.put("network", "network");

    DataprocConf conf = DataprocConf.fromProperties(props);
    Assert.assertNull(conf.getZone());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testInvalidZoneCheck() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("zone", "region2-a");
    props.put("network", "network");

    DataprocConf.fromProperties(props);
  }
}

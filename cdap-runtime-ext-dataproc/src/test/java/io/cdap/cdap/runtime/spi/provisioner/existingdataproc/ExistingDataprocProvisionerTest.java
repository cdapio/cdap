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

package io.cdap.cdap.runtime.spi.provisioner.existingdataproc;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for ExistingDataproc provisioner
 */
public class ExistingDataprocProvisionerTest {


  @Test
  public void testExistingDataprocConf() {
    Map<String, String> props = new HashMap<>();
    props.put(ExistingDataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("clusterName", "mycluster");
    props.put("labels", "joblabel1|jobvalue1;joblabel2|jobvalue2");

    ExistingDataprocConf conf = ExistingDataprocConf.fromProperties(props);

    Assert.assertEquals(conf.getProjectId(), "pid");
    Assert.assertEquals(conf.getRegion(), "region1");
    Assert.assertEquals(conf.getClusterName(), "mycluster");

    Map<String, String> labels = conf.getLabels();
    Assert.assertEquals(2, labels.size());

    Assert.assertEquals("jobvalue2", labels.get("joblabel2"));
    Assert.assertEquals("jobvalue1", labels.get("joblabel1"));
  }


  @Test(expected = IllegalArgumentException.class)
  public void testClusterName() {
    Map<String, String> props = new HashMap<>();
    props.put(ExistingDataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    ExistingDataprocConf.fromProperties(props);

  }

  @Test(expected = IllegalArgumentException.class)
  public void testLabelKey() {
    Map<String, String> props = new HashMap<>();
    props.put(ExistingDataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("labels", "j@oblabel1|jobvalue1;joblabel2|jobvalue2");
    ExistingDataprocConf.fromProperties(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLabelValue() {
    Map<String, String> props = new HashMap<>();
    props.put(ExistingDataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("labels", "joblabel1|jo#bvalue1;joblabel2|jobvalue2");
    ExistingDataprocConf.fromProperties(props);
  }

}

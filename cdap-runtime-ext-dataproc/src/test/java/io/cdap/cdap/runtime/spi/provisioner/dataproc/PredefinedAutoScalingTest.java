/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.dataproc.v1.AutoscalingPolicy;
import com.google.cloud.dataproc.v1.AutoscalingPolicyName;
import com.google.cloud.dataproc.v1.AutoscalingPolicyServiceClient;
import com.google.cloud.dataproc.v1.RegionName;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AutoscalingPolicyServiceClient.class})
public class PredefinedAutoScalingTest {

  private static final String PROJECT = "dummy-project";
  private static final String REGION = "dummy-region";

  DataprocConf dataprocConf;

  @Before
  public void init() {
    Map<String, String> properties = new HashMap<>();
    properties.put("accountKey", "{ \"type\": \"test\"}");
    properties.put(DataprocConf.PROJECT_ID_KEY, PROJECT);
    properties.put("zone", REGION);
    dataprocConf = DataprocConf.create(properties);
  }

  @Test
  public void testGeneratedAutoScalePolicyPublicConfigs() {
    PredefinedAutoScaling predefinedAutoScaling = new PredefinedAutoScaling(dataprocConf);
    AutoscalingPolicy autoscalingPolicy = predefinedAutoScaling.generatePredefinedAutoScaling();

    Assert.assertEquals(PredefinedAutoScaling.AUTOSCALING_POLICY_ID, autoscalingPolicy.getId());
    Assert.assertEquals(PredefinedAutoScaling.getPrimaryWorkerInstances(),
                        autoscalingPolicy.getWorkerConfig().getMaxInstances());
    Assert.assertEquals(PredefinedAutoScaling.getPrimaryWorkerInstances(),
                        autoscalingPolicy.getWorkerConfig().getMinInstances());
    Assert.assertEquals(PredefinedAutoScaling.getMinSecondaryWorkerInstances(),
                        autoscalingPolicy.getSecondaryWorkerConfig().getMinInstances());
    Assert.assertEquals(PredefinedAutoScaling.getMaxSecondaryWorkerInstances(),
                        autoscalingPolicy.getSecondaryWorkerConfig().getMaxInstances());

  }

  @Test
  public void testFetchingExistingAutoScalingPolicySuccess() throws IOException {
    PredefinedAutoScaling predefinedAutoScaling = new PredefinedAutoScaling(dataprocConf);

    //mock Return generated auto-scaling policy while fetching
    AutoscalingPolicyName autoscalingPolicyName = AutoscalingPolicyName.ofProjectLocationAutoscalingPolicyName(
      dataprocConf.getProjectId(), dataprocConf.getRegion(), PredefinedAutoScaling.AUTOSCALING_POLICY_ID);

    AutoscalingPolicy generatedPolicy = predefinedAutoScaling.generatePredefinedAutoScaling();
    AutoscalingPolicyServiceClient mockClient = PowerMockito.mock(AutoscalingPolicyServiceClient.class);
    Mockito.when(mockClient.getAutoscalingPolicy(autoscalingPolicyName))
      .thenReturn(generatedPolicy);

    PredefinedAutoScaling spy = Mockito.spy(predefinedAutoScaling);
    Mockito.doReturn(mockClient).when(spy).getAutoscalingPolicyServiceClient();

    String name = spy.createPredefinedAutoScalingPolicy();

    Mockito.verify(mockClient, Mockito.times(1)).getAutoscalingPolicy(autoscalingPolicyName);
    //verify that create call is not made
    RegionName parent = RegionName.of(dataprocConf.getProjectId(), dataprocConf.getRegion());
    Mockito.verify(mockClient, Mockito.never()).createAutoscalingPolicy(parent, generatedPolicy);
    Assert.assertEquals(name, autoscalingPolicyName.toString());
  }

  @Test
  public void testFetchFailedAndCreateIsCalled() throws IOException {
    PredefinedAutoScaling predefinedAutoScaling = new PredefinedAutoScaling(dataprocConf);

    //mock Return generated auto-scaling policy while fetching
    AutoscalingPolicyName autoscalingPolicyName = AutoscalingPolicyName.ofProjectLocationAutoscalingPolicyName(
      dataprocConf.getProjectId(), dataprocConf.getRegion(), PredefinedAutoScaling.AUTOSCALING_POLICY_ID);

    AutoscalingPolicy generatedPolicy = predefinedAutoScaling.generatePredefinedAutoScaling();
    AutoscalingPolicyServiceClient mockClient = PowerMockito.mock(AutoscalingPolicyServiceClient.class);

    Mockito.when(mockClient.getAutoscalingPolicy(autoscalingPolicyName)).thenThrow(NotFoundException.class);

    RegionName parent = RegionName.of(dataprocConf.getProjectId(), dataprocConf.getRegion());
    Mockito.when(mockClient.createAutoscalingPolicy(parent, generatedPolicy)).thenReturn(null);

    PredefinedAutoScaling spy = Mockito.spy(predefinedAutoScaling);
    Mockito.doReturn(mockClient).when(spy).getAutoscalingPolicyServiceClient();

    String name = spy.createPredefinedAutoScalingPolicy();

    Mockito.verify(mockClient, Mockito.times(1)).getAutoscalingPolicy(autoscalingPolicyName);
    //verify that create call is not made

    Mockito.verify(mockClient, Mockito.times(1)).createAutoscalingPolicy(parent, generatedPolicy);
    Assert.assertEquals(name, autoscalingPolicyName.toString());
  }
}

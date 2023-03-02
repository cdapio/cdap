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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.dataproc.v1.AutoscalingPolicy;
import com.google.cloud.dataproc.v1.AutoscalingPolicyName;
import com.google.cloud.dataproc.v1.AutoscalingPolicyServiceClient;
import com.google.cloud.dataproc.v1.AutoscalingPolicyServiceSettings;
import com.google.cloud.dataproc.v1.BasicAutoscalingAlgorithm;
import com.google.cloud.dataproc.v1.BasicYarnAutoscalingConfig;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.InstanceGroupAutoscalingPolicyConfig;
import com.google.cloud.dataproc.v1.RegionName;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Duration;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataproc's Auto-Scaling policy Operations and configurations to be used when a pipeline is
 * enabled with CDAP's predefined auto-scaling feature.
 */
public class PredefinedAutoScaling {

  private static final Logger LOG = LoggerFactory.getLogger(PredefinedAutoScaling.class);
  static final String AUTOSCALING_POLICY_ID = "CDF_AUTOSCALING_POLICY_V1";
  private DataprocConf dataprocConf;

  PredefinedAutoScaling(DataprocConf dataprocConf) {
    this.dataprocConf = dataprocConf;
  }

  public static int getPrimaryWorkerInstances() {
    return PredefinedAutoScalingPolicy.WorkerConfig.MIN_INSTANCES;
  }

  public static int getMinSecondaryWorkerInstances() {
    return PredefinedAutoScalingPolicy.SecondaryWorkerConfig.MIN_INSTANCES;
  }

  public static int getMaxSecondaryWorkerInstances() {
    return PredefinedAutoScalingPolicy.SecondaryWorkerConfig.MAX_INSTANCES;
  }

  //Builds the auto scaling policy according to cdap's predefined values.
  @VisibleForTesting
  AutoscalingPolicy generatePredefinedAutoScaling() {
    InstanceGroupAutoscalingPolicyConfig workerInstanceGroupAutoscalingPolicyConfig =
        InstanceGroupAutoscalingPolicyConfig.newBuilder()
            .setMinInstances(PredefinedAutoScalingPolicy.WorkerConfig.MIN_INSTANCES)
            .setMaxInstances(PredefinedAutoScalingPolicy.WorkerConfig.MAX_INSTANCES)
            .setWeight(1)
            .build();

    InstanceGroupAutoscalingPolicyConfig secondaryWorkerInstanceGroupAutoscalingPolicyConfig =
        InstanceGroupAutoscalingPolicyConfig.newBuilder()
            .setMinInstances(PredefinedAutoScalingPolicy.SecondaryWorkerConfig.MIN_INSTANCES)
            .setMaxInstances(PredefinedAutoScalingPolicy.SecondaryWorkerConfig.MAX_INSTANCES)
            .setWeight(1)
            .build();

    BasicYarnAutoscalingConfig basicYarnApplicationConfig =
        BasicYarnAutoscalingConfig.newBuilder()
            .setScaleUpFactor(PredefinedAutoScalingPolicy.BasicAlgorithm.YarnConfig.SCALE_UP_FACTOR)
            .setScaleDownFactor(
                PredefinedAutoScalingPolicy.BasicAlgorithm.YarnConfig.SCALE_DOWN_FACTOR)
            .setScaleUpMinWorkerFraction(
                PredefinedAutoScalingPolicy.BasicAlgorithm.YarnConfig.SCALE_UP_MIN_WORKER_FRACTION)
            .setGracefulDecommissionTimeout(
                PredefinedAutoScalingPolicy.BasicAlgorithm.YarnConfig.GRACEFUL_DECOMMISSION)
            .build();

    BasicAutoscalingAlgorithm basicAutoscalingAlgorithm =
        BasicAutoscalingAlgorithm.newBuilder()
            .setYarnConfig(basicYarnApplicationConfig)
            .build();

    AutoscalingPolicy autoscalingPolicy =
        AutoscalingPolicy.newBuilder()
            .setId(AUTOSCALING_POLICY_ID)
            .setWorkerConfig(workerInstanceGroupAutoscalingPolicyConfig)
            .setSecondaryWorkerConfig(secondaryWorkerInstanceGroupAutoscalingPolicyConfig)
            .setBasicAlgorithm(basicAutoscalingAlgorithm)
            .build();

    return autoscalingPolicy;
  }

  //Creates the auto-scaling policy if doesn't exist.
  public String createPredefinedAutoScalingPolicy() throws IOException {
    AutoscalingPolicyName autoscalingPolicyName = AutoscalingPolicyName.ofProjectLocationAutoscalingPolicyName(
        dataprocConf.getProjectId(), dataprocConf.getRegion(), AUTOSCALING_POLICY_ID);
    AutoscalingPolicy generatedPolicy = generatePredefinedAutoScaling();

    try (AutoscalingPolicyServiceClient autoscalingPolicyServiceClient = getAutoscalingPolicyServiceClient()) {

      //If the policy already exists meaning it is already created by cdap or in rare scenario the customer might have
      //created with a similar name. It is also possible that the user might have changed configs in the autoscaling
      //policy.
      // So, if exists -> we will fetch and check if it differs from our predefined values and LOG it
      AutoscalingPolicy existingPolicy = null;
      boolean createPolicy = false;
      try {
        existingPolicy = autoscalingPolicyServiceClient.getAutoscalingPolicy(autoscalingPolicyName);
        boolean yarnDiff = !existingPolicy.getBasicAlgorithm().getYarnConfig()
            .equals(generatedPolicy.getBasicAlgorithm().getYarnConfig());
        boolean workerDiff = !existingPolicy.getWorkerConfig()
            .equals(generatedPolicy.getWorkerConfig());
        boolean secondaryWorkerDiff = !existingPolicy.getSecondaryWorkerConfig()
            .equals(generatedPolicy.getSecondaryWorkerConfig());

        if (yarnDiff || workerDiff || secondaryWorkerDiff) {
          LOG.warn(
              "The predefined auto-scaling policy {} already exists and is having a different configuration"

                  + "as compared to CDF/CDAP's chosen configuration", existingPolicy.getName());
        }
      } catch (NotFoundException e) {
        createPolicy = true;
        LOG.debug("The autoscaling policy doesn't exists");
      }

      if (createPolicy) {
        RegionName parent = RegionName.of(dataprocConf.getProjectId(), dataprocConf.getRegion());
        try {
          autoscalingPolicyServiceClient.createAutoscalingPolicy(parent, generatedPolicy);
        } catch (AlreadyExistsException e) {
          // no - op Race condition: in case 2 pipelines with auto-scaling are triggered together and might try to
          // create in parallel
        }
      }
    }
    return autoscalingPolicyName.toString();
  }

  /*
   * Using the input Google Credentials retrieve the Dataproc Autoscaling Service client
   */
  @VisibleForTesting
  AutoscalingPolicyServiceClient getAutoscalingPolicyServiceClient()
      throws IOException {
    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(
        dataprocConf.getDataprocCredentials());

    String rootUrl = Optional.ofNullable(dataprocConf.getRootUrl())
        .orElse(ClusterControllerSettings.getDefaultEndpoint());
    String regionalEndpoint = dataprocConf.getRegion() + "-" + rootUrl;

    AutoscalingPolicyServiceSettings autoscalingPolicyServiceSettings = AutoscalingPolicyServiceSettings.newBuilder()
        .setCredentialsProvider(credentialsProvider)
        .setEndpoint(regionalEndpoint)
        .build();
    return AutoscalingPolicyServiceClient.create(autoscalingPolicyServiceSettings);
  }

  /**
   * The Auto Scaling Profile configurations chosen according to POC conducted
   */
  private static final class PredefinedAutoScalingPolicy {

    private static final class BasicAlgorithm {

      private static final class YarnConfig {

        private static final Double SCALE_UP_FACTOR = 0.2;
        private static final Double SCALE_DOWN_FACTOR = 0.0;
        private static final Double SCALE_UP_MIN_WORKER_FRACTION = 0.75;
        private static final Duration GRACEFUL_DECOMMISSION = Duration.newBuilder()
            .setSeconds(86400).build();
      }
    }

    private static final class WorkerConfig {

      private static final int MIN_INSTANCES = 2;
      private static final int MAX_INSTANCES = 2;
    }

    private static final class SecondaryWorkerConfig {

      private static final int MIN_INSTANCES = 0;
      private static final int MAX_INSTANCES = 40;
    }
  }
}

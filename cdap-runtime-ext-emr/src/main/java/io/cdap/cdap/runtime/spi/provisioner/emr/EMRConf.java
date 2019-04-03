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

package io.cdap.cdap.runtime.spi.provisioner.emr;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Configuration for EMR.
 */
public class EMRConf {
  private final String accessKey;
  private final String secretKey;
  private final String region;
  private final String ec2SubnetId;
  private final String additionalMasterSecurityGroup;
  private final String serviceRole;
  private final String jobFlowRole;
  private final String logURI;
  private final String masterInstanceType;
  private final String workerInstanceType;
  private final int instanceCount;

  private final SSHPublicKey publicKey;

  private EMRConf(String accessKey, String secretKey, String region, String ec2SubnetId,
                  String additionalMasterSecurityGroup, String serviceRole, String jobFlowRole,
                  String masterInstanceType, String workerInstanceType, int instanceCount,
                  @Nullable String logURI, @Nullable SSHPublicKey publicKey) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.region = region;
    this.ec2SubnetId = ec2SubnetId;
    this.additionalMasterSecurityGroup = additionalMasterSecurityGroup;
    this.serviceRole = serviceRole;
    this.jobFlowRole = jobFlowRole;
    this.logURI = logURI;
    this.masterInstanceType = masterInstanceType;
    this.workerInstanceType = workerInstanceType;
    this.instanceCount = instanceCount;
    this.publicKey = publicKey;
  }

  String getRegion() {
    return region;
  }

  String getEc2SubnetId() {
    return ec2SubnetId;
  }

  String getAdditionalMasterSecurityGroup() {
    return additionalMasterSecurityGroup;
  }

  String getServiceRole() {
    return serviceRole;
  }

  String getJobFlowRole() {
    return jobFlowRole;
  }

  int getInstanceCount() {
    return instanceCount;
  }

  String getMasterInstanceType() {
    return masterInstanceType;
  }

  String getWorkerInstanceType() {
    return workerInstanceType;
  }

  @Nullable
  String getLogURI() {
    return logURI;
  }

  @Nullable
  SSHPublicKey getPublicKey() {
    return publicKey;
  }

  AWSCredentialsProvider getCredentialsProvider() {
    return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
  }

  static EMRConf fromProvisionerContext(ProvisionerContext context) {
    Optional<SSHKeyPair> sshKeyPair = context.getSSHContext().getSSHKeyPair();
    return create(context.getProperties(), sshKeyPair.map(SSHKeyPair::getPublicKey).orElse(null));
  }

  /**
   * Create the conf from a property map while also performing validation.
   */
  static EMRConf fromProperties(Map<String, String> properties) {
    return create(properties, null);
  }

  private static EMRConf create(Map<String, String> properties, @Nullable SSHPublicKey publicKey) {
    String accessKey = getString(properties, "accessKey");
    String secretKey = getString(properties, "secretKey");

    String region = getString(properties, "region", Regions.DEFAULT_REGION.getName());

    String ec2SubnetId = getString(properties, "ec2SubnetId");
    String additionalMasterSecurityGroup = getString(properties, "additionalMasterSecurityGroup");
    String serviceRole = getString(properties, "serviceRole", "EMR_DefaultRole");
    String jobFlowRole = getString(properties, "jobFlowRole", "EMR_EC2_DefaultRole");

    String logURI = getString(properties, "logURI", null);

    String masterInstanceType = getString(properties, "masterInstanceType", "m1.medium");
    String workerInstanceType = getString(properties, "workerInstanceType", "m1.medium");
    int instanceCount = getInt(properties, "instanceCount", 3);

    return new EMRConf(accessKey, secretKey, region, ec2SubnetId,
                       additionalMasterSecurityGroup, serviceRole, jobFlowRole,
                       masterInstanceType, workerInstanceType, instanceCount,
                       logURI, publicKey);
  }

  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val == null) {
      throw new IllegalArgumentException(String.format("Invalid config. '%s' must be specified.", key));
    }
    return val;
  }

  private static String getString(Map<String, String> properties, String key, String defaultVal) {
    String val = properties.get(key);
    return val == null ? defaultVal : val;
  }

  private static int getInt(Map<String, String> properties, String key, int defaultVal) {
    String valStr = properties.get(key);
    if (valStr == null) {
      return defaultVal;
    }
    try {
      int val = Integer.parseInt(valStr);
      if (val < 0) {
        throw new IllegalArgumentException(
          String.format("Invalid config '%s' = '%s'. Must be a positive integer.", key, valStr));
      }
      return val;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Invalid config '%s' = '%s'. Must be a valid, positive integer.", key, valStr));
    }
  }
}

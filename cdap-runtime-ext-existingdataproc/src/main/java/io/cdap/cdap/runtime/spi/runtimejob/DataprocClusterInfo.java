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

package io.cdap.cdap.runtime.spi.runtimejob;

import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to carry information about dataproc cluster. Instance of this class will be created by provisioner.
 */
public class DataprocClusterInfo {

  private final ProvisionerContext provisionerContext;
  private final String clusterName;
  private final String endpoint;
  private final GoogleCredentials credentials;
  private final String projectId;
  private final String region;
  private final String bucket;
  private final Map<String, String> labels;

  public DataprocClusterInfo(ProvisionerContext provisionerContext,
                             String clusterName, GoogleCredentials credentials, String endpoint, String projectId,
                             String region, String bucket, Map<String, String> labels) {
    this.provisionerContext = provisionerContext;
    this.clusterName = clusterName;
    this.endpoint = endpoint;
    this.credentials = credentials;
    this.projectId = projectId;
    this.region = region;
    this.bucket = bucket;
    this.labels = Collections.unmodifiableMap(new HashMap<>(labels));
  }

  ProvisionerContext getProvisionerContext() {
    return provisionerContext;
  }

  String getClusterName() {
    return clusterName;
  }

  String getEndpoint() {
    return endpoint;
  }

  GoogleCredentials getCredentials() {
    return credentials;
  }

  String getProjectId() {
    return projectId;
  }

  String getRegion() {
    return region;
  }

  String getBucket() {
    return bucket;
  }

  Map<String, String> getLabels() {
    return labels;
  }
}

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

package io.cdap.cdap.k8s.common;

import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentCondition;
import io.kubernetes.client.models.V1DeploymentStatus;
import io.kubernetes.client.models.V1ObjectMeta;

import java.util.List;

/**
 * Metadata provider for K8s resource of type Deployment.
 */
public class DeploymentMetaProvider implements ObjectMetaProvider<V1Deployment> {

  private static final String AVAILABLE_TYPE = "Available";

  @Override
  public V1ObjectMeta getObjectMeta(V1Deployment resource) {
    return resource.getMetadata();
  }

  @Override
  public boolean isObjectAvailable(V1Deployment resource) {
    V1DeploymentStatus status = resource.getStatus();
    if (status == null) {
      return false;
    }
    List<V1DeploymentCondition> conditions = status.getConditions();
    if (conditions == null) {
      return false;
    }

    return conditions.stream()
      .filter(c -> AVAILABLE_TYPE.equals(c.getType()))
      .map(V1DeploymentCondition::getStatus)
      .findFirst()
      .map(Boolean::parseBoolean)
      .orElse(false);
  }
}

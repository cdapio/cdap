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

import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1StatefulSetCondition;
import io.kubernetes.client.models.V1StatefulSetStatus;

import java.util.List;

/**
 * Metadata provider for K8s resource of type StatefulSet.
 */
public class StatefulSetMetaProvider  implements ObjectMetaProvider<V1StatefulSet> {

  private static final String AVAILABLE_TYPE = "Available";

  @Override
  public V1ObjectMeta getObjectMeta(V1StatefulSet resource) {
    return resource.getMetadata();
  }

  @Override
  public boolean isObjectAvailable(V1StatefulSet resource) {
    V1StatefulSetStatus status = resource.getStatus();
    if (status == null) {
      return false;
    }
    List<V1StatefulSetCondition> conditions = status.getConditions();
    if (conditions == null) {
      return false;
    }
    return conditions.stream()
      .filter(c -> AVAILABLE_TYPE.equals(c.getType()))
      .map(V1StatefulSetCondition::getStatus)
      .findFirst()
      .map(Boolean::parseBoolean)
      .orElse(false);
  }
}

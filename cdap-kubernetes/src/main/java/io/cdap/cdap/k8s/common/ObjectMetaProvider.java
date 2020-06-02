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

/**
 * K8s resources metadata provider.
 *
 * @param <T> type of the resource
 */
public interface ObjectMetaProvider<T> {
  /**
   * Get the metadata.
   * @param resource the resource for which metadata to be provided
   * @return the instance of V1ObjectMeta for the resource
   */
  V1ObjectMeta getObjectMeta(T resource);

  /**
   * Check if the K8s resource is available.
   * @param resource resource which need to be checked for availability
   * @return {@code true} if the resource status is {@code Available}, otherwise return {@code false}
   */
  boolean isObjectAvailable(T resource);
}

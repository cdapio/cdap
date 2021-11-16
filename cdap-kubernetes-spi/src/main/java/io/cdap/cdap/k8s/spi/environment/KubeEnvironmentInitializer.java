/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.k8s.spi.environment;

import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1PodSpec;

import java.util.Map;

/**
 * Pluggable SPI for initializing the Kubernetes master environment.
 */
public interface KubeEnvironmentInitializer {

  /**
   * Returns the specification for the k8s environment initializer.
   *
   * @return The k8s environment initializer specification
   */
  KubeEnvironmentInitializerSpecification getSpecification();

  /**
   * Called by KubeMasterEnvironment after the onNamespaceCreation hook is called.
   *
   * @param namespace The name of the namespace
   * @param properties The properties for the namespace
   * @param coreV1Api The reference used to call into k8s core APIs
   */
  void postNamespaceCreation(String namespace, Map<String, String> properties, CoreV1Api coreV1Api) throws Exception;

  /**
   * Called by KubeMasterEnvironment to modify the pod spec template for the driver pod used by Spark on Kubernetes.
   *
   * @param podSpec The pod specification to modify
   */
  void modifySparkDriverPodTemplate(V1PodSpec podSpec) throws Exception;

  /**
   * Called by KubeMasterEnvironment to modify the pod spec template for the executor pod used by Spark on Kubernetes.
   *
   * @param podSpec The pod specification to modify
   */
  void modifySparkExecutorPodTemplate(V1PodSpec podSpec) throws Exception;
}

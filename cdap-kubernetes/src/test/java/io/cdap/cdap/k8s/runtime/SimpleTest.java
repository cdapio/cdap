/*
 * Copyright © 2019-2021 Cask Data, Inc.
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
package io.cdap.cdap.k8s.runtime;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobBuilder;
import io.kubernetes.client.util.Config;

import java.util.Collections;

public class SimpleTest {
  public static void main(String[] args) throws Exception {
    ApiClient apiClient = Config.defaultClient();
    BatchV1Api batchApi = new BatchV1Api(apiClient.setDebugging(true));

    V1Job job = new V1JobBuilder()
      .withApiVersion("batch/v1")
      .withNewMetadata()
      .withName("pi-two")
      .withLabels(Collections.singletonMap("label1", "maximum-length-of-63-characters"))
      .withAnnotations(Collections.singletonMap("annotation1", "some-very-long-annotation"))
      .endMetadata()
      .withNewSpec()
      .withParallelism(1)
      .withCompletions(1)
      .withBackoffLimit(0)
      .withNewTemplate()
      .withNewSpec()
      .addNewContainer()
      .withName("pi")
      .withImage("perl")
      .withArgs("perl", "-Mbignum=bpi", "-wle", "print bpi(2000)")
      .endContainer()
      .withRestartPolicy("Never")
      .endSpec()
      .endTemplate()
      .endSpec()
      .build();
    job = batchApi.createNamespacedJob("default", job, "true", null, null);

  }
}

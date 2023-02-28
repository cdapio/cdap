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

package io.cdap.cdap.k8s.runtime;

import static org.mockito.Mockito.mock;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.twill.api.ServiceController;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

public class KubeTwillControllerTest {
  @Test
  public void testTerminate() {
    DiscoveryServiceClient mockDiscoveryServiceClient = mock(DiscoveryServiceClient.class);
    ApiClient mockApiClient = mock(ApiClient.class);
    // obj meta with disable cleanup annotation
    V1ObjectMeta objMetaWithAnnotation = new V1ObjectMeta().name("test-job-name")
      .putAnnotationsItem(KubeTwillRunnerService.RUNTIME_CLEANUP_DISABLED, "true");
    CompletableFuture<Void> startupTaskCompletion = new CompletableFuture<>();
    KubeTwillController controller = new KubeTwillController("default", null, mockDiscoveryServiceClient,
                                                             mockApiClient, V1Job.class, objMetaWithAnnotation,
                                                             startupTaskCompletion);
    V1JobStatus jobStatus = new V1JobStatus();
    jobStatus.setFailed(1);
    controller.setJobStatus(jobStatus);

    Future<? extends ServiceController> terminateFuture = controller.terminate();
    Assert.assertTrue(terminateFuture.isDone());
  }
}

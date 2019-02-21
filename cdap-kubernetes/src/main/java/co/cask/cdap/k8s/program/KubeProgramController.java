/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.k8s.program;

import co.cask.cdap.master.spi.program.AbstractProgramController;
import co.cask.cdap.proto.id.ProgramRunId;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1DeleteOptions;

/**
 * Kubernetes program controller.
 */
public class KubeProgramController extends AbstractProgramController {
  private final String kubeNamespace;
  private final ApiClient apiClient;

  public KubeProgramController(ProgramRunId programRunId, String kubeNamespace, ApiClient apiClient) {
    super(programRunId);
    this.kubeNamespace = kubeNamespace;
    this.apiClient = apiClient;
  }

  @Override
  protected void doSuspend() {
    throw new UnsupportedOperationException("Suspend is not currently allowed for programs running on Kubernetes");
  }

  @Override
  protected void doResume() {
    throw new UnsupportedOperationException("Resume is not currently allowed for programs running on Kubernetes");
  }

  @Override
  protected void doStop() throws Exception {
    // delete the deployment, then delete the config map
    String resourceName = KubernetesPrograms.getResourceName(getProgramRunId());
    V1DeleteOptions deleteOptions = new V1DeleteOptions();
    AppsV1Api appsApi = new AppsV1Api(apiClient);
    appsApi.deleteNamespacedDeployment(resourceName, kubeNamespace, deleteOptions, null, null, null, null);

    CoreV1Api coreApi = new CoreV1Api(apiClient);
    coreApi.deleteNamespacedConfigMap(resourceName, kubeNamespace, deleteOptions, null, null, null, null);
  }

  @Override
  protected void doCommand(String name, Object value) {
    // no-op
  }
}

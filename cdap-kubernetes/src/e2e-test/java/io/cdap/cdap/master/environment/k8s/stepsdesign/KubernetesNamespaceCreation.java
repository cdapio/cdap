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

package io.cdap.cdap.master.environment.k8s.stepsdesign;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.k8s.runtime.KubeTwillRunnerService;
import io.cdap.cdap.master.environment.k8s.actions.NamespaceCreationActions;
import io.cdap.e2e.pages.actions.CdfSysAdminActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.util.Map;

/**
 * Namespace creation related steps definitions.
 */
public class KubernetesNamespaceCreation implements CdfHelper {

  private static final int WAIT_TIME_MS = 3000;

  private static CoreV1Api coreV1Api;

  @Given("Get Kubernetes coreV1Api")
  public static void getCoreV1Api() throws IOException {
    coreV1Api = new CoreV1Api(Config.defaultClient());
  }

  @When("Open namespace creation wizard")
  public static void beginNamespaceCreation() {
    CdfSysAdminActions.clickSystemAdminMenu();
    CdfSysAdminActions.clickConfigurationMenu();
    NamespaceCreationActions.openNamespaceCreationWizard();
  }

  @Then("Enter namespace name {string}")
  public static void enterNamespaceName(String name) {
    NamespaceCreationActions.enterNamespaceName(name);
  }

  @Then("Enter Kubernetes namespace name {string}")
  public static void enterKubernetesNamespaceName(String name) {
    NamespaceCreationActions.enterKubernetesNamespace(name);
  }

  @Then("Enter CPU limit {string}")
  public static void enterKubernetesCpuLimit(String limit) {
    NamespaceCreationActions.enterKubernetesCpu(limit);
  }

  @Then("Enter memory limit {string}")
  public static void enterKubernetesMemoryLimit(String limit) {
    NamespaceCreationActions.enterKubernetesMemory(limit);
  }

  @Then("Go to next page in namespace creation wizard")
  public static void nextNamespaceCreationPage() {
    NamespaceCreationActions.clickNextButton();
  }

  @Then("Finish namespace creation")
  public static void finishNamespaceCreation() throws InterruptedException {
    NamespaceCreationActions.clickFinishButton();
    // add delay to wait for Kubernetes actions to complete
    Thread.sleep(WAIT_TIME_MS);
  }

  @Then("Verify Kubernetes namespace {string} exists")
  public static void verifyKubeNamespaceExists(String namespace) throws IOException {
    try {
      coreV1Api.readNamespace(namespace, null);
    } catch (ApiException e) {
      throw new IOException("Error occurred while checking for Kubernetes namespace. Error code = "
                              + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
  }

  @Then("Verify CPU limit {string} and memory limit {string} exists for {string}")
  public static void verifyResourceLimits(String cpuLimit, String memLimit, String namespace) throws IOException {
    Map<String, Quantity> hardLimitMap = ImmutableMap.of("limits.cpu", new Quantity(cpuLimit), "limits.memory",
                                                         new Quantity(memLimit));
    try {
      V1ResourceQuota resourceQuota = coreV1Api.readNamespacedResourceQuota(KubeTwillRunnerService.RESOURCE_QUOTA_NAME,
                                                                            namespace, null);
      if (resourceQuota.getSpec() == null) {
        throw new IOException("Resource quota not created");
      } else if (!hardLimitMap.equals(resourceQuota.getSpec().getHard())) {
        throw new IOException(String.format("Incorrect resource limits. Expected %s but received %s",
                                            hardLimitMap, resourceQuota.getSpec().getHard()));
      }
    } catch (ApiException e) {
      throw new IOException("Error occurred while checking for Kubernetes resource quota. Error code = "
                              + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
  }

  @Then("Verify namespace creation failure")
  public static void verifyNamespaceCreationFailure() throws IOException {
    if (!NamespaceCreationActions.isNamespaceCreationFailed()) {
      throw new IOException("Namespace creation did not fail as expected");
    }
  }
}

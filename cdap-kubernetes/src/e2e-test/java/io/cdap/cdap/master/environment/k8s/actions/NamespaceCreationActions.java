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

package io.cdap.cdap.master.environment.k8s.actions;

import io.cdap.cdap.master.environment.k8s.locators.NamespaceCreationLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;

/**
 * Namespace creation related actions.
 */
public class NamespaceCreationActions {

  static {
    SeleniumHelper.getPropertiesLocators(NamespaceCreationLocators.class);
  }

  public static void openNamespaceCreationWizard() {
    ElementHelper.clickOnElement(NamespaceCreationLocators.createNamespaceWizard);
  }

  public static void clickNextButton() {
    ElementHelper.clickOnElement(NamespaceCreationLocators.nextButton);
  }

  public static void clickFinishButton() {
    ElementHelper.clickOnElement(NamespaceCreationLocators.finishButton);
  }

  public static void enterNamespaceName(String name) {
    ElementHelper.replaceElementValue(NamespaceCreationLocators.namespaceNameInput, name);
  }

  public static void enterKubernetesNamespace(String name) {
    ElementHelper.replaceElementValue(NamespaceCreationLocators.kubernetesNamespaceInput, name);
  }

  public static void enterKubernetesCpu(String limit) {
    ElementHelper.replaceElementValue(NamespaceCreationLocators.kubernetesCpuInput, limit);
  }

  public static void enterKubernetesMemory(String limit) {
    ElementHelper.replaceElementValue(NamespaceCreationLocators.kubernetesMemoryInput, limit);
  }

  public static boolean isNamespaceCreationFailed() {
    return ElementHelper.isElementDisplayed(NamespaceCreationLocators.namespaceCreationFailure);
  }
}

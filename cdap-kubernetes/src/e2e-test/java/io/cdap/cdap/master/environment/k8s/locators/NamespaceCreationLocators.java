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

package io.cdap.cdap.master.environment.k8s.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Namespace creation related locators.
 */
public class NamespaceCreationLocators {

  @FindBy(how = How.XPATH, using = "//button[contains(text(),'Create New Namespace')]")
  public static WebElement createNamespaceWizard;

  @FindBy(how = How.XPATH, using = "//button[@data-cy='wizard-next-btn']")
  public static WebElement nextButton;

  @FindBy(how = How.XPATH, using = "//button[@data-cy='wizard-finish-btn']")
  public static WebElement finishButton;

  @FindBy(how = How.XPATH, using = "//input[@placeholder='Namespace name']")
  public static WebElement namespaceNameInput;

  @FindBy(how = How.XPATH, using = "//input[contains(@placeholder, 'Kubernetes namespace')]")
  public static WebElement kubernetesNamespaceInput;

  @FindBy(how = How.XPATH, using = "//input[contains(@placeholder, 'CPU limit')]")
  public static WebElement kubernetesCpuInput;

  @FindBy(how = How.XPATH, using = "//input[contains(@placeholder, 'Memory limit')]")
  public static WebElement kubernetesMemoryInput;

  @FindBy(how = How.XPATH, using = "//*[contains(text(), 'Failed to Add namespace')]")
  public static WebElement namespaceCreationFailure;

}

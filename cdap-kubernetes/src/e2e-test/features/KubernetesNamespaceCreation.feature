#
# Copyright © 2022 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

@KubernetesNamespace_Creation
Feature: Kubernetes namespace - Validate Kubernetes namespace creation scenarios

  @KUBE_NAMESPACE_TEST
  Scenario: Validate successful Kubernetes namespace creation
    Given Open Datafusion Project to configure pipeline
    And Get Kubernetes coreV1Api
    When Open namespace creation wizard
    Then Enter namespace name "test_cdap_namespace"
    Then Go to next page in namespace creation wizard
    Then Enter Kubernetes namespace name "test-kube-namespace"
    Then Finish namespace creation
    Then Verify Kubernetes namespace "test-kube-namespace" exists

  @KUBE_NAMESPACE_TEST
  Scenario: Validate successful Kubernetes namespace creation with resource limits
    Given Open Datafusion Project to configure pipeline
    And Get Kubernetes coreV1Api
    When Open namespace creation wizard
    Then Enter namespace name "test_cdap_namespace_with_limits"
    Then Go to next page in namespace creation wizard
    Then Enter Kubernetes namespace name "test-kube-namespace-with-limits"
    Then Enter CPU limit "2"
    Then Enter memory limit "2Gi"
    Then Finish namespace creation
    Then Verify Kubernetes namespace "test-kube-namespace-with-limits" exists
    Then Verify CPU limit "2" and memory limit "2Gi" exists for "test-kube-namespace-with-limits"

  @KUBE_NAMESPACE_TEST
  Scenario: Validate unsuccessful namespace creation with invalid Kubernetes name
    Given Open Datafusion Project to configure pipeline
    When Open namespace creation wizard
    Then Enter namespace name "test_cdap_namespace_2"
    Then Go to next page in namespace creation wizard
    Then Enter Kubernetes namespace name "test_invalid_kube_namespace"
    Then Finish namespace creation
    Then Verify namespace creation failure

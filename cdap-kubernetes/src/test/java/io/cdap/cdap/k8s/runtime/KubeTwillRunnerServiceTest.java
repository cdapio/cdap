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

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.cdap.cdap.master.environment.k8s.DefaultApiClientFactory;
import io.cdap.cdap.master.environment.k8s.KubeMasterEnvironment;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.namespace.NamespaceDetail;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ClusterRoleBindingList;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSecurityContext;
import io.kubernetes.client.openapi.models.V1RoleBindingList;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KubeTwillRunnerServiceTest {
  private static final String CDAP_NAMESPACE = "TEST_CDAP_Namespace";
  private static final String KUBE_NAMESPACE = "test-kube-namespace";

  private CoreV1Api coreV1Api;
  private RbacAuthorizationV1Api rbacV1Api;
  private KubeTwillRunnerService twillRunnerService;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void init() {
    coreV1Api = mock(CoreV1Api.class);
    rbacV1Api = mock(RbacAuthorizationV1Api.class);
    PodInfo podInfo = new PodInfo("test-pod-name", "test-pod-dir", "test-label-file.txt",
                                  "test-name-file.txt", "test-pod-uid", "test-uid-file.txt", "test-namespace-file.txt",
                                  "test-pod-namespace", Collections.emptyMap(), Collections.emptyList(),
                                  "test-pod-service-account", "test-pod-runtime-class",
                                  Collections.emptyList(), "test-pod-container-label", "test-pod-container-image",
                                  Collections.emptyList(), Collections.emptyList(), new V1PodSecurityContext(),
                                  "test-pod-image-pull-policy");
    MasterEnvironmentContext context = mock(MasterEnvironmentContext.class);
    DiscoveryServiceClient discoveryServiceClient = mock(DiscoveryServiceClient.class);
    ApiClientFactory apiClientFactory = new DefaultApiClientFactory(10, 300);
    twillRunnerService = new KubeTwillRunnerService(context, apiClientFactory,
                                                    KUBE_NAMESPACE, discoveryServiceClient, podInfo,
                                                    "", Collections.emptyMap(),
                                                    true, false,
                                                    null, null,
                                                    null, null);
    twillRunnerService.setCoreV1Api(coreV1Api);
    twillRunnerService.setRbacV1Api(rbacV1Api);
  }

  @Test
  public void testOnNamespaceCreationWithNoNamespace() throws Exception {
    Map<String, String> properties = new HashMap<>();
    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Cannot create Kubernetes namespace for %s because no name was provided",
                                       CDAP_NAMESPACE));
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);
    twillRunnerService.onNamespaceCreation(namespaceDetail);
  }

  @Test
  public void testOnNamespaceCreationWithBootstrapNamespace() {
    Map<String, String> properties = new HashMap<>();
    NamespaceDetail namespaceDetail = new NamespaceDetail("default", properties);
    try {
      twillRunnerService.onNamespaceCreation(namespaceDetail);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error for bootstrap namespace. Exception: " + e);
    }
  }

  @Test
  public void testOnNamespaceCreationWithExistingNamespace() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    V1Namespace returnedNamespace = new V1Namespace().metadata(new V1ObjectMeta().name(KUBE_NAMESPACE));
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any())).thenReturn(returnedNamespace);

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Kubernetes namespace %s exists but was not created by CDAP", KUBE_NAMESPACE));
    twillRunnerService.onNamespaceCreation(namespaceDetail);
  }

  @Test
  public void testOnNamespaceCreationWithExistingNamespaceAndWrongCdapInstance() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    V1ObjectMeta returnedMeta = new V1ObjectMeta().name(KUBE_NAMESPACE)
      .putLabelsItem("cdap.namespace", "wrong namespace");
    V1Namespace returnedNamespace = new V1Namespace().metadata(returnedMeta);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any())).thenReturn(returnedNamespace);

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Kubernetes namespace %s exists but was not created by CDAP namespace %s",
                                       KUBE_NAMESPACE, CDAP_NAMESPACE));
    twillRunnerService.onNamespaceCreation(namespaceDetail);
  }

  @Test
  public void testOnNamespaceCreationWithBadKubeName() throws Exception {
    Set<String> badKubeNames = ImmutableSet.of(CDAP_NAMESPACE, "UPPERCASE", "special*char", "-badstart", "badend-");
    Map<String, String> properties = new HashMap<>();
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    for (String kubeName : badKubeNames) {
      try {
        properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, kubeName);
        twillRunnerService.onNamespaceCreation(namespaceDetail);
        Assert.fail(String.format("%s does not meet Kubernetes naming standards", kubeName));
      } catch (IllegalArgumentException e) {
        // ignore
      }
    }
  }

  @Test
  public void testOnNamespaceCreationSuccess() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    skipRbacSteps();
    try {
      twillRunnerService.onNamespaceCreation(namespaceDetail);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
    verify(coreV1Api, times(0)).createNamespacedConfigMap(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testOnNamespaceCreationWithSuppressedDeletionError() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    V1ObjectMeta returnedMeta = new V1ObjectMeta().name(KUBE_NAMESPACE)
      .putLabelsItem("cdap.namespace", CDAP_NAMESPACE);
    V1Namespace returnedNamespace = new V1Namespace().metadata(returnedMeta);

    // throw ApiException when coreV1Api.readNamespace() is called in findOrCreateKubeNamespace()
    // return returnedNamespace when coreV1Api.readNamespace() is called in deleteKubeNamespace()
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"))
      .thenReturn(returnedNamespace);
    when(coreV1Api.createNamespace(any(), any(), any(), any(), any()))
      .thenThrow(new ApiException());
    when(coreV1Api.deleteNamespace(eq(KUBE_NAMESPACE), any(), any(), any(), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_INTERNAL_ERROR, "internal error message"));

    try {
      twillRunnerService.onNamespaceCreation(namespaceDetail);
    } catch (IOException e) {
      Assert.assertThat(e.getMessage(),
                        CoreMatchers.containsString("Error occurred while creating Kubernetes namespace"));
      Assert.assertEquals(1, e.getCause().getSuppressed().length);
      Assert.assertThat(e.getCause().getSuppressed()[0].getMessage(),
                        CoreMatchers.containsString("Error occurred while deleting Kubernetes namespace."));
    }
  }

  @Test
  public void testOnNamespaceDeletionWithKubernetesNotFoundError() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    try {
      twillRunnerService.onNamespaceDeletion(namespaceDetail);
    } catch (Exception e) {
      Assert.fail("Kubernetes deletion should not error if namespace does not exist. Exception: " + e);
    }
  }

  @Test
  public void testOnNamespaceCreationWithWorkloadIdentityEnabled() throws Exception {
    String workloadIdentityGCPServiceAccount = "test-service-account@test-project-id.iam.gserviceaccount.com";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    properties.put(KubeTwillRunnerService.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
                   workloadIdentityGCPServiceAccount);
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    enableWorkloadIdentity();

    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    when(coreV1Api.readNamespacedConfigMap(eq(KUBE_NAMESPACE), eq("workload-identity-config"), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "config map not found"));
    skipRbacSteps();
    try {
      twillRunnerService.onNamespaceCreation(namespaceDetail);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
    verify(coreV1Api, times(1)).createNamespacedConfigMap(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testOnNamespaceCreationWithWorkloadIdentityEnabledDoesNotCreateExistingConfigMap() throws Exception {
    String workloadIdentityGCPServiceAccount = "test-service-account@test-project-id.iam.gserviceaccount.com";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    properties.put(KubeTwillRunnerService.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
                   workloadIdentityGCPServiceAccount);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    enableWorkloadIdentity();

    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    when(coreV1Api.readNamespacedConfigMap(eq(KUBE_NAMESPACE), eq("workload-identity-config"), any()))
      .thenReturn(new V1ConfigMap().metadata(new V1ObjectMeta().name("workload-identity-config")
                                               .namespace(KUBE_NAMESPACE)));
    skipRbacSteps();
    try {
      twillRunnerService.onNamespaceCreation(namespaceDetail);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
    verify(coreV1Api, times(0)).createNamespacedConfigMap(any(), any(), any(), any(), any(), any());
  }

  @Test(expected = IOException.class)
  public void testOnNamespaceCreationWithWorkloadIdentityEnabledReadConfigMapPropagatesException() throws Exception {
    String workloadIdentityGCPServiceAccount = "test-service-account@test-project-id.iam.gserviceaccount.com";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    properties.put(KubeTwillRunnerService.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
                   workloadIdentityGCPServiceAccount);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    enableWorkloadIdentity();

    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    when(coreV1Api.readNamespacedConfigMap(any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_INTERNAL_ERROR, "internal error"));
    when(coreV1Api.createNamespacedConfigMap(any(), any(), any(), any(), any(), any())).thenThrow(new ApiException());
    skipRbacSteps();
    twillRunnerService.onNamespaceCreation(namespaceDetail);
  }

  @Test(expected = ApiException.class)
  public void testOnNamespaceCreationWithWorkloadIdentityEnabledCreateNamespacePropagatesException() throws Exception {
    String workloadIdentityGCPServiceAccount = "test-service-account@test-project-id.iam.gserviceaccount.com";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    properties.put(KubeTwillRunnerService.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
                   workloadIdentityGCPServiceAccount);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    enableWorkloadIdentity();

    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    when(coreV1Api.readNamespacedConfigMap(eq(KUBE_NAMESPACE), eq("workload-identity-config"), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "config map not found"));
    when(coreV1Api.createNamespacedConfigMap(any(), any(), any(), any(), any(), any())).thenThrow(new ApiException());
    skipRbacSteps();
    twillRunnerService.onNamespaceCreation(namespaceDetail);
  }

  @Test
  public void testOnNamespaceCreationWithWorkloadIdentityEnabledNoServiceAccountPropertyCreatesNoVolumes()
    throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    NamespaceDetail namespaceDetail = new NamespaceDetail(CDAP_NAMESPACE, properties);

    enableWorkloadIdentity();

    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    skipRbacSteps();
    try {
      twillRunnerService.onNamespaceCreation(namespaceDetail);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
    verify(coreV1Api, times(0)).createNamespacedConfigMap(any(), any(), any(), any(), any(), any());
    verify(coreV1Api, times(0)).readNamespacedConfigMap(any(), any(), any());

  }

  private void enableWorkloadIdentity() {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityProvider = "https://gkehub.googleapis.com/projects/test-project-id/locations/global/" +
      "memberships/test-cluster";
    twillRunnerService.setWorkloadIdentityEnabled();
    twillRunnerService.setWorkloadIdentityPool(workloadIdentityPool);
    twillRunnerService.setWorkloadIdentityProvider(workloadIdentityProvider);
  }

  private void skipRbacSteps() throws ApiException {
    when(rbacV1Api.listNamespacedRoleBinding(eq(KUBE_NAMESPACE), any(), any(), any(), any(), any(), any(), any(),
                                             any(), any(), any()))
      .thenReturn(new V1RoleBindingList());
    when(rbacV1Api.listClusterRoleBinding(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(new V1ClusterRoleBindingList());
  }
}

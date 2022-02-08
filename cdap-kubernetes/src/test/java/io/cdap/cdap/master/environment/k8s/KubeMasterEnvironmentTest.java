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

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.k8s.common.TemporaryLocalFileProvider;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.master.spi.environment.spark.SparkSubmitContext;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapProjection;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1KeyToPath;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1ProjectedVolumeSource;
import io.kubernetes.client.openapi.models.V1ServiceAccountTokenProjection;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeProjection;
import io.kubernetes.client.util.Yaml;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link KubeMasterEnvironment}
 */
public class KubeMasterEnvironmentTest {

  private static final String CDAP_NAMESPACE = "TEST_CDAP_Namespace";
  private static final String KUBE_NAMESPACE = "test-kube-namespace";

  private CoreV1Api coreV1Api;
  private KubeMasterEnvironment kubeMasterEnvironment;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws IOException {
    coreV1Api = mock(CoreV1Api.class);
    kubeMasterEnvironment = new KubeMasterEnvironment();
    kubeMasterEnvironment.setCoreV1Api(coreV1Api);
    kubeMasterEnvironment.setNamespaceCreationEnabled();
    kubeMasterEnvironment.setLocalFileProvider(new TemporaryLocalFileProvider(temporaryFolder));
    KubeMasterPathProvider mockKubeMasterPathProvider = mock(KubeMasterPathProvider.class);
    when(mockKubeMasterPathProvider.getMasterPath()).thenReturn("https://127.0.0.1:443");
    kubeMasterEnvironment.setKubeMasterPathProvider(mockKubeMasterPathProvider);
    kubeMasterEnvironment.setAdditionalSparkConfs(Collections.emptyMap());
    // Create a dummy file for the pod
    File dummyFile = temporaryFolder.newFile();
    kubeMasterEnvironment.setPodInfoDir(new File("/tmp/"));
    kubeMasterEnvironment.setPodLabelsFile(dummyFile);
    kubeMasterEnvironment.setPodNameFile(dummyFile);
    kubeMasterEnvironment.setPodUidFile(dummyFile);
    kubeMasterEnvironment.setPodInfo(new PodInfo("pod-info", "pod-info-dir", dummyFile.getAbsolutePath(),
                                                 dummyFile.getAbsolutePath(), UUID.randomUUID().toString(),
                                                 dummyFile.getAbsolutePath(), KUBE_NAMESPACE, Collections.emptyMap(),
                                                 Collections.emptyList(), "service-account", "runtime-class",
                                                 Collections.emptyList(), "container-label-name", "container-image",
                                                 Collections.emptyList(), Collections.emptyList(), null,
                                                 "image-pull-policy"));
  }

  @Test
  public void testOnNamespaceCreationWithNoNamespace() throws Exception {
    Map<String, String> properties = new HashMap<>();
    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Cannot create Kubernetes namespace for %s because no name was provided",
            CDAP_NAMESPACE));
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationWithBootstrapNamespace() {
    Map<String, String> properties = new HashMap<>();
    try {
      kubeMasterEnvironment.onNamespaceCreation("default", properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error for bootstrap namespace. Exception: " + e);
    }
  }

  @Test
  public void testOnNamespaceCreationWithExistingNamespace() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    V1Namespace returnedNamespace = new V1Namespace().metadata(new V1ObjectMeta().name(KUBE_NAMESPACE));
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any())).thenReturn(returnedNamespace);

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Kubernetes namespace %s exists but was not created by CDAP", KUBE_NAMESPACE));
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationWithExistingNamespaceAndWrongCdapInstance() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    V1ObjectMeta returnedMeta = new V1ObjectMeta().name(KUBE_NAMESPACE)
      .putLabelsItem("cdap.namespace", "wrong namespace");
    V1Namespace returnedNamespace = new V1Namespace().metadata(returnedMeta);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any())).thenReturn(returnedNamespace);

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Kubernetes namespace %s exists but was not created by CDAP namespace %s",
                                       KUBE_NAMESPACE, CDAP_NAMESPACE));
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationSuccess() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
    verify(coreV1Api, times(0)).createNamespacedConfigMap(any(), any(), any(), any(), any());
  }

  @Test
  public void testOnNamespaceCreationWithSuppressedDeletionError() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    V1ObjectMeta returnedMeta = new V1ObjectMeta().name(KUBE_NAMESPACE)
      .putLabelsItem("cdap.namespace", CDAP_NAMESPACE);
    V1Namespace returnedNamespace = new V1Namespace().metadata(returnedMeta);

    // throw ApiException when coreV1Api.readNamespace() is called in findOrCreateKubeNamespace()
    // return returnedNamespace when coreV1Api.readNamespace() is called in deleteKubeNamespace()
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"))
      .thenReturn(returnedNamespace);
    when(coreV1Api.createNamespace(any(), any(), any(), any()))
      .thenThrow(new ApiException());
    when(coreV1Api.deleteNamespace(eq(KUBE_NAMESPACE), any(), any(), any(), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_INTERNAL_ERROR, "internal error message"));

    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
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
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    try {
      kubeMasterEnvironment.onNamespaceDeletion(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes deletion should not error if namespace does not exist. Exception: " + e);
    }
  }

  @Test
  public void testOnNamespaceCreationWithWorkloadIdentityEnabled() throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityGCPServiceAccount = "test-service-account@test-project-id.iam.gserviceaccount.com";
    String workloadIdentityProvider = "https://gkehub.googleapis.com/projects/test-project-id/locations/global/" +
      "memberships/test-cluster";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    properties.put(KubeMasterEnvironment.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
                   workloadIdentityGCPServiceAccount);
    kubeMasterEnvironment.setNamespaceCreationEnabled();
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityProvider(workloadIdentityProvider);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    when(coreV1Api.readNamespacedConfigMap(eq(KUBE_NAMESPACE), eq("workload-identity-config"), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "config map not found"));
    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
    verify(coreV1Api, times(1)).createNamespacedConfigMap(any(), any(), any(), any(), any());
  }

  @Test
  public void testOnNamespaceCreationWithWorkloadIdentityEnabledDoesNotCreateExistingConfigMap() throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityGCPServiceAccount = "test-service-account@test-project-id.iam.gserviceaccount.com";
    String workloadIdentityProvider = "https://gkehub.googleapis.com/projects/test-project-id/locations/global/" +
      "memberships/test-cluster";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    properties.put(KubeMasterEnvironment.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
                   workloadIdentityGCPServiceAccount);
    kubeMasterEnvironment.setNamespaceCreationEnabled();
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityProvider(workloadIdentityProvider);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    when(coreV1Api.readNamespacedConfigMap(eq(KUBE_NAMESPACE), eq("workload-identity-config"), any(), any(), any()))
      .thenReturn(new V1ConfigMap().metadata(new V1ObjectMeta().name("workload-identity-config")
                                               .namespace(KUBE_NAMESPACE)));
    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
    verify(coreV1Api, times(0)).createNamespacedConfigMap(any(), any(), any(), any(), any());
  }


  @Test(expected = IOException.class)
  public void testOnNamespaceCreationWithWorkloadIdentityEnabledReadConfigMapPropagatesException() throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityGCPServiceAccount = "test-service-account@test-project-id.iam.gserviceaccount.com";
    String workloadIdentityProvider = "https://gkehub.googleapis.com/projects/test-project-id/locations/global/" +
      "memberships/test-cluster";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    properties.put(KubeMasterEnvironment.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
                   workloadIdentityGCPServiceAccount);
    kubeMasterEnvironment.setNamespaceCreationEnabled();
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityProvider(workloadIdentityProvider);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    when(coreV1Api.readNamespacedConfigMap(any(), any(), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_INTERNAL_ERROR, "internal error"));
    when(coreV1Api.createNamespacedConfigMap(any(), any(), any(), any(), any())).thenThrow(new ApiException());
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test(expected = ApiException.class)
  public void testOnNamespaceCreationWithWorkloadIdentityEnabledCreateNamespacePropagatesException() throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityGCPServiceAccount = "test-service-account@test-project-id.iam.gserviceaccount.com";
    String workloadIdentityProvider = "https://gkehub.googleapis.com/projects/test-project-id/locations/global/" +
      "memberships/test-cluster";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    properties.put(KubeMasterEnvironment.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
                   workloadIdentityGCPServiceAccount);
    kubeMasterEnvironment.setNamespaceCreationEnabled();
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityProvider(workloadIdentityProvider);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    when(coreV1Api.readNamespacedConfigMap(eq(KUBE_NAMESPACE), eq("workload-identity-config"), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "config map not found"));
    when(coreV1Api.createNamespacedConfigMap(any(), any(), any(), any(), any())).thenThrow(new ApiException());
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationWithWorkloadIdentityEnabledNoServiceAccountPropertyCreatesNoVolumes()
    throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityProvider = "https://gkehub.googleapis.com/projects/test-project-id/locations/global/" +
      "memberships/test-cluster";
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    kubeMasterEnvironment.setNamespaceCreationEnabled();
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityProvider(workloadIdentityProvider);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
    verify(coreV1Api, times(0)).createNamespacedConfigMap(any(), any(), any(), any(), any());
    verify(coreV1Api, times(0)).readNamespacedConfigMap(any(), any(), any(), any(), any());

  }

  private void assertContainsVolume(V1Pod pod, V1Volume expectedVolume) {
    for (V1Volume volume : pod.getSpec().getVolumes()) {
      if (volume.getName().equals(expectedVolume.getName())) {
        Assert.assertEquals(expectedVolume, volume);
        return;
      }
    }
    Assert.fail(String.format("Expected volume '%s' not found in pod spec '%s'", expectedVolume.getName(),
                              pod.getMetadata().getName()));
  }

  private void assertContainersContainVolumeMount(V1Pod pod, V1VolumeMount expectedVolumeMount) {
    for (V1Container container : pod.getSpec().getContainers()) {
      for (V1VolumeMount volumeMount : container.getVolumeMounts()) {
        if (volumeMount.getName().equals(expectedVolumeMount.getName())) {
          Assert.assertEquals(expectedVolumeMount, volumeMount);
          return;
        }
      }
      Assert.fail(String.format("Expected volume mount '%s' not found in container '%s' for pod '%s'",
                                expectedVolumeMount.getName(),
                                container.getName(),
                                pod.getMetadata().getName()));
    }
  }

  private void assertContainersContainEnvVar(V1Pod pod, V1EnvVar expectedEnvVar) {
    for (V1Container container : pod.getSpec().getContainers()) {
      for (V1EnvVar envVar : container.getEnv()) {
        if (envVar.getName().equals(expectedEnvVar.getName())) {
          Assert.assertEquals(expectedEnvVar, envVar);
          return;
        }
      }
      Assert.fail(String.format("Expected environment variable '%s' not found in container '%s' for pod '%s'",
                                expectedEnvVar.getName(),
                                container.getName(),
                                pod.getMetadata().getName()));
    }
  }

  @Test
  public void testGenerateSparkConfigWithWorkloadIdentityEnabled() throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityProvider = "https://gkehub.googleapis.com/projects/test-project-id/locations/global/" +
      "memberships/test-cluster";
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityProvider(workloadIdentityProvider);
    kubeMasterEnvironment.setWorkloadIdentityServiceAccountTokenTTLSeconds(172800L);

    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap());

    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);

    // Verify volume, volume mount, and environment variables are set for workload identity
    Map<String, String> configs = sparkConfig.getConfigs();
    File sparkDriverPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_DRIVER_POD_TEMPLATE));
    File sparkExecutorPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE));
    V1Pod driverPod = Yaml.loadAs(sparkDriverPodFile, V1Pod.class);
    V1Pod executorPod = Yaml.loadAs(sparkExecutorPodFile, V1Pod.class);

    // NOTE: Several of these values are hard-coded to ensure that it is not changed by accident as it can cause other
    // pieces to fail. If one of the values changed, the other constants must be validated!
    V1ServiceAccountTokenProjection workloadIdentityKSATokenProjection = new V1ServiceAccountTokenProjection()
      .path("token")
      .expirationSeconds(172800L)
      .audience(workloadIdentityPool);
    V1ConfigMapProjection workloadIdentityGSAConfigMapProjection = new V1ConfigMapProjection()
      .name("workload-identity-config")
      .optional(false)
      .addItemsItem(new V1KeyToPath().key("config").path("google-application-credentials.json"));
    V1Volume expectedWorkloadIdentityProjectedVolume = new V1Volume().name("gcp-ksa")
      .projected(new V1ProjectedVolumeSource()
                   .defaultMode(420)
                   .addSourcesItem(new V1VolumeProjection().serviceAccountToken(workloadIdentityKSATokenProjection))
                   .addSourcesItem(new V1VolumeProjection().configMap(workloadIdentityGSAConfigMapProjection)));
    V1VolumeMount expectedWorkloadIdentityProjectedVolumeMount = new V1VolumeMount().name("gcp-ksa")
      .mountPath("/var/run/secrets/tokens/gcp-ksa").readOnly(true);
    V1EnvVar expectedGoogleApplicationCredentialsEnvVar = new V1EnvVar().name("GOOGLE_APPLICATION_CREDENTIALS")
      .value("/var/run/secrets/tokens/gcp-ksa/google-application-credentials.json");
    // Verify pod specs
    assertContainsVolume(driverPod, expectedWorkloadIdentityProjectedVolume);
    assertContainersContainVolumeMount(driverPod, expectedWorkloadIdentityProjectedVolumeMount);
    assertContainersContainEnvVar(driverPod, expectedGoogleApplicationCredentialsEnvVar);
    assertContainsVolume(executorPod, expectedWorkloadIdentityProjectedVolume);
    assertContainersContainVolumeMount(executorPod, expectedWorkloadIdentityProjectedVolumeMount);
    assertContainersContainEnvVar(executorPod, expectedGoogleApplicationCredentialsEnvVar);
  }

  @Test
  public void testGetComponentName() {
    // Ensure valid pod names are parsed correctly
    String gotName = KubeMasterEnvironment.getComponentName(
      "instance-abc", "cdap-instance-abc-appfabric-0");
    Assert.assertEquals("appfabric-0", gotName);

    gotName = KubeMasterEnvironment.getComponentName(
      "123", "cdap-123-appfabric-23");
    Assert.assertEquals("appfabric-23", gotName);

    gotName = KubeMasterEnvironment.getComponentName(
      "cdap", "cdap-cdap-appfabric-0");
    Assert.assertEquals("appfabric-0", gotName);

    gotName = KubeMasterEnvironment.getComponentName(
      "", "cdap-test-metrics-0");
    Assert.assertEquals("test-metrics-0", gotName);

    gotName = KubeMasterEnvironment.getComponentName(
      "test-cdap", "cdap-test-cdap-preview-runner-b5786a15-e8f4-47-0cebad7d67-0");
    Assert.assertEquals("preview-runner-b5786a15-e8f4-47-0cebad7d67-0", gotName);

    gotName = KubeMasterEnvironment.getComponentName(
      "dap", "cdap-dap-preview-runner-b5786a15-e8f4-47-0cebad7d67-0");
    Assert.assertEquals("preview-runner-b5786a15-e8f4-47-0cebad7d67-0", gotName);
  }
}

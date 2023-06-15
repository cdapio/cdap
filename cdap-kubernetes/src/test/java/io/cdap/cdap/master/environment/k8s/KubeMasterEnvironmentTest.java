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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cdap.cdap.k8s.common.TemporaryLocalFileProvider;
import io.cdap.cdap.k8s.runtime.KubeTwillRunnerService;
import io.cdap.cdap.k8s.util.WorkloadIdentityUtil;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.master.spi.environment.spark.SparkSubmitContext;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMapProjection;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1KeyToPath;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ProjectedVolumeSource;
import io.kubernetes.client.openapi.models.V1ServiceAccountTokenProjection;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeProjection;
import io.kubernetes.client.util.Yaml;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link KubeMasterEnvironment}
 */
public class KubeMasterEnvironmentTest {

  private static final String KUBE_NAMESPACE = "test-kube-namespace";
  private static final String KUBE_INSTALL_NAMESPACE = "kube-install-namespace";

  private CoreV1Api coreV1Api;
  private KubeMasterEnvironment kubeMasterEnvironment;
  private KubeTwillRunnerService twillRunnerService;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws IOException {
    coreV1Api = mock(CoreV1Api.class);
    twillRunnerService = mock(KubeTwillRunnerService.class);
    kubeMasterEnvironment = new KubeMasterEnvironment();
    kubeMasterEnvironment.setCoreV1Api(coreV1Api);
    kubeMasterEnvironment.setTwillRunner(twillRunnerService);
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
    kubeMasterEnvironment.setPodNamespaceFile(dummyFile);
    kubeMasterEnvironment.setPodInfo(new PodInfo("pod-info", "pod-info-dir", dummyFile.getAbsolutePath(),
                                                 dummyFile.getAbsolutePath(), UUID.randomUUID().toString(),
                                                 dummyFile.getAbsolutePath(), dummyFile.getAbsolutePath(),
                                                 KUBE_NAMESPACE, Collections.emptyMap(),
                                                 Collections.emptyList(), "service-account", "runtime-class",
                                                 Collections.emptyList(), "container-label-name", "container-image",
                                                 Collections.emptyList(), Collections.emptyList(), null,
                                                 "image-pull-policy"));
    kubeMasterEnvironment.setProgramCpuMultiplier("1");
  }

  @Test
  public void testGenerateSparkConfigWithNamespace() throws Exception {
    Map<String, String> config = new HashMap<>();
    String ns = "some-ns";
    config.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, ns);
    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap(), config, 1, 1);
    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);
    Assert.assertEquals(ns, sparkConfig.getConfigs().get("spark.kubernetes.namespace"));
  }

  @Test
  public void testGenerateSparkConfigWithDriverTimeouts() throws Exception {
    Map<String, String> config = new HashMap<>();
    String requestTimeout = "20000";
    String connectTimeout = "20000";
    config.put(KubeMasterEnvironment.SPARK_DRIVER_REQUEST_TIMEOUT_MILLIS, requestTimeout);
    config.put(KubeMasterEnvironment.SPARK_DRIVER_CONNECTION_TIMEOUT_MILLIS, connectTimeout);

    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap(), config, 1, 1);
    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);
    Assert.assertEquals(requestTimeout,
                        sparkConfig.getConfigs().get(KubeMasterEnvironment.SPARK_DRIVER_REQUEST_TIMEOUT_MILLIS));
    Assert.assertEquals(connectTimeout,
                        sparkConfig.getConfigs().get(KubeMasterEnvironment.SPARK_DRIVER_CONNECTION_TIMEOUT_MILLIS));
  }

  @Test
  public void testGenerateSparkConfigWithDefaultDriverTimeouts() throws Exception {
    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap(), Collections.emptyMap(),
                                                                   1, 1);
    kubeMasterEnvironment.setConnectTimeout(Integer.parseInt(KubeMasterEnvironment.CONNECT_TIMEOUT_DEFAULT));
    kubeMasterEnvironment.setReadTimeout(Integer.parseInt(KubeMasterEnvironment.READ_TIMEOUT_DEFAULT));

    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);
    int readTimeoutMillis = Integer.parseInt(KubeMasterEnvironment.READ_TIMEOUT_DEFAULT) * 1000;
    Assert.assertEquals(String.valueOf(readTimeoutMillis),
                        sparkConfig.getConfigs().get(KubeMasterEnvironment.SPARK_DRIVER_REQUEST_TIMEOUT_MILLIS));
    int connectTimeoutMillis = Integer.parseInt(KubeMasterEnvironment.CONNECT_TIMEOUT_DEFAULT) * 1000;
    Assert.assertEquals(String.valueOf(connectTimeoutMillis),
                        sparkConfig.getConfigs().get(KubeMasterEnvironment.SPARK_DRIVER_CONNECTION_TIMEOUT_MILLIS));
  }

  @Test
  public void testGenerateSparkConfigWithWorkloadIdentityEnabledInNonInstallNamespaceMountsConfigMap()
    throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityProvider =
        "https://gkehub.googleapis.com/projects/test-project-id/locations/global/"
            + "memberships/test-cluster";
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityServiceAccountTokenTtlSeconds(172800L);
    kubeMasterEnvironment.setCdapInstallNamespace(KUBE_INSTALL_NAMESPACE);

    Map<String, String> conf = new HashMap<>();
    conf.put(KubeTwillRunnerService.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
        "test-email@gmail.com");
    conf.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap(), conf, 1,
        1);

    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);

    // Verify volume, volume mount, and environment variables are set for workload identity
    Map<String, String> configs = sparkConfig.getConfigs();
    File sparkDriverPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_DRIVER_POD_TEMPLATE));
    File sparkExecutorPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE));
    V1Pod driverPod = Yaml.loadAs(sparkDriverPodFile, V1Pod.class);
    V1Pod executorPod = Yaml.loadAs(sparkExecutorPodFile, V1Pod.class);

    assertMountsWorkloadIdentityVolume(workloadIdentityPool, driverPod, executorPod);
  }

  @Test
  public void testGenerateSparkConfigWithWorkloadIdentityEnabledInInstallNamespaceWithNoEmailMountsConfigMap()
    throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityProvider =
        "https://gkehub.googleapis.com/projects/test-project-id/locations/global/"
            + "memberships/test-cluster";
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityServiceAccountTokenTtlSeconds(172800L);
    kubeMasterEnvironment.setCdapInstallNamespace(KUBE_INSTALL_NAMESPACE);

    Map<String, String> conf = new HashMap<>();
    conf.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_INSTALL_NAMESPACE);

    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap(), conf, 1,
        1);

    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);

    // Verify volume, volume mount, and environment variables are set for workload identity
    Map<String, String> configs = sparkConfig.getConfigs();
    File sparkDriverPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_DRIVER_POD_TEMPLATE));
    File sparkExecutorPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE));
    V1Pod driverPod = Yaml.loadAs(sparkDriverPodFile, V1Pod.class);
    V1Pod executorPod = Yaml.loadAs(sparkExecutorPodFile, V1Pod.class);

    assertMountsWorkloadIdentityVolume(workloadIdentityPool, driverPod, executorPod);
  }

  @Test
  public void testGenerateSparkConfigWithWorkloadIdentityDisabledDoesNotMountConfigMap()
    throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityProvider =
        "https://gkehub.googleapis.com/projects/test-project-id/locations/global/"
            + "memberships/test-cluster";
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityServiceAccountTokenTtlSeconds(172800L);
    kubeMasterEnvironment.setCdapInstallNamespace(KUBE_INSTALL_NAMESPACE);

    Map<String, String> conf = new HashMap<>();
    conf.put(KubeTwillRunnerService.WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY,
        "test-email@gmail.com");
    conf.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap(), conf, 1,
        1);

    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);

    // Verify volume, volume mount, and environment variables are set for workload identity
    Map<String, String> configs = sparkConfig.getConfigs();
    File sparkDriverPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_DRIVER_POD_TEMPLATE));
    File sparkExecutorPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE));
    V1Pod driverPod = Yaml.loadAs(sparkDriverPodFile, V1Pod.class);
    V1Pod executorPod = Yaml.loadAs(sparkExecutorPodFile, V1Pod.class);

    assertDoesNotMountWorkloadIdentityVolume(driverPod, executorPod);
  }

  @Test
  public void testGenerateSparkConfigWithWorkloadIdentityEnabledInDifferentNamespaceWithNoEmailDoesNotMountConfigMap()
    throws Exception {
    String workloadIdentityPool = "test-workload-pool";
    String workloadIdentityProvider =
        "https://gkehub.googleapis.com/projects/test-project-id/locations/global/"
            + "memberships/test-cluster";
    kubeMasterEnvironment.setWorkloadIdentityEnabled();
    kubeMasterEnvironment.setWorkloadIdentityPool(workloadIdentityPool);
    kubeMasterEnvironment.setWorkloadIdentityServiceAccountTokenTtlSeconds(172800L);
    kubeMasterEnvironment.setCdapInstallNamespace(KUBE_INSTALL_NAMESPACE);

    Map<String, String> conf = new HashMap<>();
    conf.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap(), conf, 1,
        1);

    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);

    // Verify volume, volume mount, and environment variables are set for workload identity
    Map<String, String> configs = sparkConfig.getConfigs();
    File sparkDriverPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_DRIVER_POD_TEMPLATE));
    File sparkExecutorPodFile = new File(configs.get(KubeMasterEnvironment.SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE));
    V1Pod driverPod = Yaml.loadAs(sparkDriverPodFile, V1Pod.class);
    V1Pod executorPod = Yaml.loadAs(sparkExecutorPodFile, V1Pod.class);

    assertDoesNotMountWorkloadIdentityVolume(driverPod, executorPod);
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

  private void assertDoesNotMountWorkloadIdentityVolume(V1Pod driverPod, V1Pod executorPod) {
    List<V1Pod> pods = Arrays.asList(driverPod, executorPod);
    for (V1Pod pod : pods) {
      V1PodSpec podSpec = pod.getSpec();
      for (V1Volume volume : podSpec.getVolumes()) {
        if (volume.getName().equals(WorkloadIdentityUtil.WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME)) {
          Assert.fail(String.format("Found unexpected volume '%s' in pod spec '%s'",
                                    WorkloadIdentityUtil.WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME,
                                    pod.getMetadata().getName()));
        }
      }
      for (V1Container container : podSpec.getContainers()) {
        for (V1VolumeMount volumeMount : container.getVolumeMounts()) {
          if (volumeMount.getName().equals(WorkloadIdentityUtil.WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME)) {
            Assert.fail(String.format("Found unexpected volume mount '%s' in container '%s' in pod '%s'",
                                      WorkloadIdentityUtil.WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME,
                                      container.getName(),
                                      pod.getMetadata().getName()));
          }
        }
      }
    }
  }

  private void assertMountsWorkloadIdentityVolume(String workloadIdentityPool, V1Pod driverPod,
                                                  V1Pod executorPod) {
    // NOTE: Several of these values are hard-coded to ensure that it is not changed by accident as it can cause other
    // pieces to fail. If one of the values changed, the other constants must be validated!
    V1ServiceAccountTokenProjection workloadIdentityKsaTokenProjection = new V1ServiceAccountTokenProjection()
      .path("token")
      .expirationSeconds(172800L)
      .audience(workloadIdentityPool);
    V1ConfigMapProjection workloadIdentityGsaConfigMapProjection = new V1ConfigMapProjection()
      .name("workload-identity-config")
      .optional(false)
      .addItemsItem(new V1KeyToPath().key("config").path("google-application-credentials.json"));
    V1Volume expectedWorkloadIdentityProjectedVolume = new V1Volume().name("gcp-ksa")
      .projected(new V1ProjectedVolumeSource()
                   .defaultMode(420)
                   .addSourcesItem(new V1VolumeProjection().serviceAccountToken(workloadIdentityKsaTokenProjection))
                   .addSourcesItem(new V1VolumeProjection().configMap(workloadIdentityGsaConfigMapProjection)));
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
}

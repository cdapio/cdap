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

import io.cdap.cdap.common.internal.LocalFileProvider;
import io.cdap.cdap.k8s.spi.environment.KubeEnvironmentInitializer;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.master.spi.environment.spark.SparkSubmitContext;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.util.Yaml;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.cdap.cdap.master.environment.k8s.KubeMasterEnvironment.SPARK_KUBERNETES_DRIVER_POD_TEMPLATE;
import static io.cdap.cdap.master.environment.k8s.KubeMasterEnvironment.SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
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
  private ArrayList<File> filesToCleanup;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Provides a file in a provided temporary directory for testing.
   */
  private static class TemporaryLocalFileProvider implements LocalFileProvider {

    private final TemporaryFolder temporaryFolder;

    public TemporaryLocalFileProvider(TemporaryFolder temporaryFolder) {
      this.temporaryFolder = temporaryFolder;
    }

    @Override
    public File createNewFile(String name) throws IOException {
      return temporaryFolder.newFile(name);
    }
  }

  @Before
  public void init() throws Exception {
    coreV1Api = mock(CoreV1Api.class);
    kubeMasterEnvironment = new KubeMasterEnvironment();
    kubeMasterEnvironment.setLocalFileProvider(new TemporaryLocalFileProvider(temporaryFolder));
    kubeMasterEnvironment.setCoreV1Api(coreV1Api);
    kubeMasterEnvironment.setNamespaceCreationEnabled();
    kubeMasterEnvironment.setKubeEnvironmentInitializerProvider(new NoOpKubeEnvironmentInitializerProvider());
    KubeMasterPathProvider mockKubeMasterPathProvider = mock(KubeMasterPathProvider.class);
    when(mockKubeMasterPathProvider.getMasterPath()).thenReturn("https://127.0.0.1:443");
    kubeMasterEnvironment.setKubeMasterPathProvider(mockKubeMasterPathProvider);
    kubeMasterEnvironment.setAdditionalSparkConfs(Collections.emptyMap());
    // Create a dummy file for the pod
    File dummyFile = temporaryFolder.newFile();
    filesToCleanup = new ArrayList<>();
    filesToCleanup.add(dummyFile);
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

  @After
  public void cleanup() {
    for (File file : filesToCleanup) {
      if (!file.delete()) {
        throw new IllegalStateException(String.format("Failed to cleanup file '%s'", file.getAbsolutePath()));
      }
    }
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
  public void testOnNamespaceCreationWithSingleInitializer() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));

    ArgumentCaptor<String> namespaceCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map> propertiesCaptor = ArgumentCaptor.forClass(Map.class);
    KubeEnvironmentInitializer mockKubeEnvInitializer = mock(KubeEnvironmentInitializer.class);
    doNothing().when(mockKubeEnvInitializer).postNamespaceCreation(namespaceCaptor.capture(), propertiesCaptor.capture(), eq(coreV1Api));
    kubeMasterEnvironment.setKubeEnvironmentInitializerProvider(new KubeEnvironmentInitializerProvider() {
      @Override
      public Map<String, KubeEnvironmentInitializer> loadKubeEnvironmentInitializers() {
        Map<String, KubeEnvironmentInitializer> kubeEnvironmentInitializerMap = new HashMap<>();
        kubeEnvironmentInitializerMap.put("mock-kube-env-init-1", mockKubeEnvInitializer);
        return kubeEnvironmentInitializerMap;
      }
    });

    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }

    // Verify initializer was called
    verify(mockKubeEnvInitializer, times(1)).postNamespaceCreation(any(), any(), any());
    Assert.assertEquals(namespaceCaptor.getValue(), CDAP_NAMESPACE);
    Assert.assertEquals(propertiesCaptor.getValue(), properties);
  }

  @Test
  public void testOnNamespaceCreationWithMultipleInitializer() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));

    ArgumentCaptor<String> namespaceCaptor1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map> propertiesCaptor1 = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<String> namespaceCaptor2 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map> propertiesCaptor2 = ArgumentCaptor.forClass(Map.class);
    KubeEnvironmentInitializer mockKubeEnvInitializer1 = mock(KubeEnvironmentInitializer.class);
    doNothing().when(mockKubeEnvInitializer1).postNamespaceCreation(namespaceCaptor1.capture(), propertiesCaptor1.capture(), eq(coreV1Api));
    KubeEnvironmentInitializer mockKubeEnvInitializer2 = mock(KubeEnvironmentInitializer.class);
    doNothing().when(mockKubeEnvInitializer2).postNamespaceCreation(namespaceCaptor2.capture(), propertiesCaptor2.capture(), eq(coreV1Api));
    kubeMasterEnvironment.setKubeEnvironmentInitializerProvider(() -> {
      Map<String, KubeEnvironmentInitializer> kubeEnvironmentInitializerMap = new HashMap<>();
      kubeEnvironmentInitializerMap.put("mock-kube-env-init-1", mockKubeEnvInitializer1);
      kubeEnvironmentInitializerMap.put("mock-kube-env-init-2", mockKubeEnvInitializer2);
      return kubeEnvironmentInitializerMap;
    });

    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }

    // Verify initializers were called
    verify(mockKubeEnvInitializer1, times(1)).postNamespaceCreation(any(), any(), any());
    Assert.assertEquals(namespaceCaptor1.getValue(), CDAP_NAMESPACE);
    Assert.assertEquals(propertiesCaptor1.getValue(), properties);
    verify(mockKubeEnvInitializer2, times(1)).postNamespaceCreation(any(), any(), any());
    Assert.assertEquals(namespaceCaptor2.getValue(), CDAP_NAMESPACE);
    Assert.assertEquals(propertiesCaptor2.getValue(), properties);
  }

  @Test
  public void testOnNamespaceCreationInitializerExceptionPropagatesException() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));

    ArgumentCaptor<String> namespaceCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map> propertiesCaptor = ArgumentCaptor.forClass(Map.class);
    KubeEnvironmentInitializer mockKubeEnvInitializer = mock(KubeEnvironmentInitializer.class);
    doThrow(new Exception("post namespace creation error")).when(mockKubeEnvInitializer).postNamespaceCreation(namespaceCaptor.capture(), propertiesCaptor.capture(), eq(coreV1Api));
    kubeMasterEnvironment.setKubeEnvironmentInitializerProvider(() -> {
      Map<String, KubeEnvironmentInitializer> kubeEnvironmentInitializerMap = new HashMap<>();
      kubeEnvironmentInitializerMap.put("post-namespace-error-env-init", mockKubeEnvInitializer);
      return kubeEnvironmentInitializerMap;
    });

    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
      Assert.fail("on namespace creation is expected to throw an exception if the initializer throws an exception");
    } catch (Exception e) {
      // Expected to throw an exception
    }

    // Verify initializer was called with expected arguments
    verify(mockKubeEnvInitializer, times(1)).postNamespaceCreation(any(), any(), any());
    Assert.assertEquals(namespaceCaptor.getValue(), CDAP_NAMESPACE);
    Assert.assertEquals(propertiesCaptor.getValue(), properties);
  }

  @Test
  public void testGenerateSparkSubmitConfigWithSparkDriverModifierInitializer() throws Exception {
    String serviceAccountName = "spark-driver-modified";
    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap());
    KubeEnvironmentInitializer mockKubeEnvInitializer = mock(KubeEnvironmentInitializer.class);
    // Implement mocked method to add a service account name to the pod spec for verification
    Answer answer = invocation -> {
      V1PodSpec podSpec = (V1PodSpec) invocation.getArguments()[0];
      podSpec.setServiceAccountName(serviceAccountName);
      return null;
    };
    doAnswer(answer).when(mockKubeEnvInitializer).modifySparkDriverPodTemplate(any());
    doNothing().when(mockKubeEnvInitializer).modifySparkExecutorPodTemplate(any());
    kubeMasterEnvironment.setKubeEnvironmentInitializerProvider(() -> {
      Map<String, KubeEnvironmentInitializer> kubeEnvironmentInitializerMap = new HashMap<>();
      kubeEnvironmentInitializerMap.put("spark-driver-template-modifier", mockKubeEnvInitializer);
      return kubeEnvironmentInitializerMap;
    });

    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);

    // Verify the service account name was set
    Map<String, String> configs = sparkConfig.getConfigs();
    File sparkDriverPodFile = new File(configs.get(SPARK_KUBERNETES_DRIVER_POD_TEMPLATE));
    File sparkExecutorPodFile = new File(configs.get(SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE));
    filesToCleanup.add(sparkDriverPodFile);
    filesToCleanup.add(sparkExecutorPodFile);
    V1Pod pod = Yaml.loadAs(sparkDriverPodFile, V1Pod.class);
    Assert.assertEquals(pod.getSpec().getServiceAccountName(), serviceAccountName);
  }

  @Test(expected = IllegalStateException.class)
  public void testGenerateSparkSubmitConfigPropagatesSparkDriverModifierInitializerException() throws Exception {
    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap());
    KubeEnvironmentInitializer mockKubeEnvInitializer = mock(KubeEnvironmentInitializer.class);
    doThrow(new IllegalStateException("kube initializer spark driver modifier exception")).when(mockKubeEnvInitializer).modifySparkDriverPodTemplate(any());
    doNothing().when(mockKubeEnvInitializer).modifySparkExecutorPodTemplate(any());
    kubeMasterEnvironment.setKubeEnvironmentInitializerProvider(() -> {
      Map<String, KubeEnvironmentInitializer> kubeEnvironmentInitializerMap = new HashMap<>();
      kubeEnvironmentInitializerMap.put("spark-driver-modifier-exception-thrower", mockKubeEnvInitializer);
      return kubeEnvironmentInitializerMap;
    });

    kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);
  }

  @Test
  public void testGenerateSparkSubmitConfigWithSparkExecutorModifierInitializer() throws Exception {
    String serviceAccountName = "spark-executor-modified";
    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap());
    KubeEnvironmentInitializer mockKubeEnvInitializer = mock(KubeEnvironmentInitializer.class);
    // Implement mocked method to add a service account name to the pod spec for verification
    Answer answer = invocation -> {
      V1PodSpec podSpec = (V1PodSpec) invocation.getArguments()[0];
      podSpec.setServiceAccountName(serviceAccountName);
      return null;
    };
    doNothing().when(mockKubeEnvInitializer).modifySparkDriverPodTemplate(any());
    doAnswer(answer).when(mockKubeEnvInitializer).modifySparkExecutorPodTemplate(any());
    kubeMasterEnvironment.setKubeEnvironmentInitializerProvider(() -> {
      Map<String, KubeEnvironmentInitializer> kubeEnvironmentInitializerMap = new HashMap<>();
      kubeEnvironmentInitializerMap.put("spark-executor-template-modifier", mockKubeEnvInitializer);
      return kubeEnvironmentInitializerMap;
    });

    SparkConfig sparkConfig = kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);

    // Verify the service account name was set
    Map<String, String> configs = sparkConfig.getConfigs();
    File sparkDriverPodFile = new File(configs.get(SPARK_KUBERNETES_DRIVER_POD_TEMPLATE));
    File sparkExecutorPodFile = new File(configs.get(SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE));
    filesToCleanup.add(sparkDriverPodFile);
    filesToCleanup.add(sparkExecutorPodFile);
    V1Pod pod = Yaml.loadAs(sparkExecutorPodFile, V1Pod.class);
    Assert.assertEquals(pod.getSpec().getServiceAccountName(), serviceAccountName);
  }

  @Test(expected = IllegalStateException.class)
  public void testGenerateSparkSubmitConfigPropagatesSparkExecutorModifierInitializerException() throws Exception {
    SparkSubmitContext sparkSubmitContext = new SparkSubmitContext(Collections.emptyMap());
    KubeEnvironmentInitializer mockKubeEnvInitializer = mock(KubeEnvironmentInitializer.class);
    doNothing().when(mockKubeEnvInitializer).modifySparkDriverPodTemplate(any());
    doThrow(new IllegalStateException("kube initializer spark driver modifier exception")).when(mockKubeEnvInitializer).modifySparkExecutorPodTemplate(any());
    kubeMasterEnvironment.setKubeEnvironmentInitializerProvider(() -> {
      Map<String, KubeEnvironmentInitializer> kubeEnvironmentInitializerMap = new HashMap<>();
      kubeEnvironmentInitializerMap.put("spark-executor-exception-thrower", mockKubeEnvInitializer);
      return kubeEnvironmentInitializerMap;
    });

    kubeMasterEnvironment.generateSparkSubmitConfig(sparkSubmitContext);
  }
}

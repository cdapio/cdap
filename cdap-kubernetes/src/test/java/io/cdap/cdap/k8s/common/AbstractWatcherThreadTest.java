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

package io.cdap.cdap.k8s.common;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.generic.options.ListOptions;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Unit test for the {@link AbstractWatcherThread} class. The test requires a kubernetes cluster that it can test
 * against. The configuration of the kubernetes cluster has to be provided by either of the following properties:
 *
 * <ul>
 *   <li>kube.config - The kubernetes config as a string; or</li>
 *   <li>kube.config.file - The file path to the kubernetes config file</li>
 * </ul>
 *
 * An optional {@code kube.namespace} property can be provided for the test to operate on.
 */
public class AbstractWatcherThreadTest {

  private static final String LABEL_KEY = "test.configmap";

  private static ApiClientFactory apiClientFactory;
  private static ApiClient apiClient;
  private static String namespace;

  @Rule
  public final ResourceCleanupRule cleanupRule = new ResourceCleanupRule();

  @BeforeClass
  public static void detectEnvironment() throws IOException {
    String kubeConfig = System.getProperty("kube.config");
    String kubeConfigFile = System.getProperty("kube.config.file");

    Assume.assumeTrue(kubeConfig != null || kubeConfigFile != null);

    try (InputStream is = openKubeConfig(kubeConfig, kubeConfigFile)) {
      apiClient = Config.fromConfig(is);
    }

    apiClientFactory = () -> apiClient;

    namespace = System.getProperty("kube.namespace", "default");
  }

  @After
  public void cleanup() throws ApiException {
    new CoreV1Api(apiClient).deleteCollectionNamespacedConfigMap(namespace, null, null, null, null, null, LABEL_KEY,
                                                                 null, null, null, null, null, null, null);
  }

  @Test
  public void testBasicWatch() throws Exception {
    Map<String, V1ConfigMap> configMapNames = new ConcurrentHashMap<>();

    AbstractWatcherThread<V1ConfigMap> watcherThread = cleanupRule.register(new AbstractWatcherThread<V1ConfigMap>(
      "test", namespace, "", "v1", "configmaps", apiClientFactory) {

      @Override
      protected void updateListOptions(ListOptions options) {
        options.setLabelSelector(LABEL_KEY);
      }

      @Override
      public void resourceAdded(V1ConfigMap resource) {
        configMapNames.put(resource.getMetadata().getName(), resource);
      }

      @Override
      public void resourceModified(V1ConfigMap resource) {
        configMapNames.put(resource.getMetadata().getName(), resource);
      }

      @Override
      public void resourceDeleted(V1ConfigMap resource) {
        configMapNames.remove(resource.getMetadata().getName());
      }
    });

    CoreV1Api coreV1Api = new CoreV1Api(apiClient);

    // Create a config before starting the thread
    coreV1Api.createNamespacedConfigMap(namespace, createConfigMap("test", "key", "value"), null, null, null, null);

    // Start the thread.
    watcherThread.start();

    // The "before" configmap should be added
    V1ConfigMap configMap = waitFor(() -> configMapNames.get("test"), Objects::nonNull, 5, TimeUnit.SECONDS);
    String name = configMap.getMetadata().getName();

    // Update the configmap and should see the changes
    configMap.data(ImmutableMap.of("key", "newvalue"));
    coreV1Api.replaceNamespacedConfigMap(name, namespace, configMap, null, null, null, null);

    waitFor(() -> configMapNames.get(name), c -> "newvalue".equals(c.getData().get("key")), 5, TimeUnit.SECONDS);

    // Delete the configmap. A deletion should be notified
    coreV1Api.deleteNamespacedConfigMap(name,
                                        namespace, null, null, null, null, null, null);

    waitFor(() -> !configMapNames.containsKey(name), Boolean::booleanValue, 5, TimeUnit.SECONDS);
  }

  @Test
  public void testResetWatch() throws Exception {
    // This is to test CDAP-19134 to make sure no deletion events are lost
    // Before the fix, this test has non-zero chance to fail during the loop.
    Map<String, V1ConfigMap> configMapNames = new ConcurrentHashMap<>();

    AbstractWatcherThread<V1ConfigMap> watcherThread = cleanupRule.register(new AbstractWatcherThread<V1ConfigMap>(
      "test", namespace, "", "v1", "configmaps", apiClientFactory) {

      @Override
      protected void updateListOptions(ListOptions options) {
        options.setLabelSelector(LABEL_KEY);
      }

      @Override
      public void resourceAdded(V1ConfigMap resource) {
        configMapNames.put(resource.getMetadata().getName(), resource);
      }

      @Override
      public void resourceModified(V1ConfigMap resource) {
        configMapNames.put(resource.getMetadata().getName(), resource);
      }

      @Override
      public void resourceDeleted(V1ConfigMap resource) {
        configMapNames.remove(resource.getMetadata().getName());
      }
    });

    watcherThread.start();

    CoreV1Api coreV1Api = new CoreV1Api(apiClient);

    // Create and delete configmap multiple times with a watch reset in between.
    // This is to make sure during the watch reset, we are not losing any deletion events.
    for (int i = 0; i < 50; i++) {
      coreV1Api.createNamespacedConfigMap(namespace, createConfigMap("test", "key", "value"), null, null, null, null);
      V1ConfigMap configMap = waitFor(() -> configMapNames.get("test"), Objects::nonNull, 5, TimeUnit.SECONDS);
      String name = configMap.getMetadata().getName();
      watcherThread.closeWatch();
      coreV1Api.deleteNamespacedConfigMap(name, namespace, null, null, null, null, null, null);
      waitFor(() -> !configMapNames.containsKey(name), Boolean::booleanValue, 5, TimeUnit.SECONDS);
    }
  }

  private V1ConfigMap createConfigMap(String name, String...data) {
    if (data.length % 2 != 0) {
      throw new IllegalArgumentException("Data to config map should be key value pairs");
    }

    V1ConfigMapBuilder builder = new V1ConfigMapBuilder()
      .withNewMetadata()
      .withName(name)
      .withLabels(ImmutableMap.of(LABEL_KEY, name))
      .endMetadata();

    for (int i = 0; i < data.length - 1; i += 2) {
      builder.addToData(data[i], data[i + 1]);
    }

    return builder.build();
  }

  private <V> V waitFor(Callable<V> callable, Predicate<V> predicate, long timeout,
                        TimeUnit timeoutUnit) throws ExecutionException, TimeoutException, InterruptedException {
    long start = System.nanoTime();
    while (true) {
      try {
        V result = callable.call();
        if (predicate.test(result)) {
          return result;
        }
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
      long duration = timeoutUnit.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      if (duration >= timeout) {
        throw new TimeoutException(String.format("Task not completed after %d %s", timeout, timeoutUnit));
      }
      TimeUnit.MILLISECONDS.sleep(200);
    }
  }

  private static InputStream openKubeConfig(@Nullable String kubeConfig,
                                            @Nullable String kubeConfigFile) throws IOException {
    if (kubeConfig != null) {
      return new ByteArrayInputStream(kubeConfig.getBytes(StandardCharsets.UTF_8));
    }
    return new FileInputStream(kubeConfigFile);
  }
}

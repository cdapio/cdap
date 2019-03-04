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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.k8s.discovery.KubeDiscoveryService;
import co.cask.cdap.master.spi.environment.MasterEnvironment;
import co.cask.cdap.master.spi.environment.MasterEnvironmentContext;
import co.cask.cdap.master.spi.environment.MasterEnvironmentTask;
import com.google.common.base.Strings;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1OwnerReference;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.util.Config;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of {@link MasterEnvironment} to provide the environment for running in Kubernetes.
 */
public class KubeMasterEnvironment implements MasterEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(KubeMasterEnvironment.class);

  private static final String NAMESPACE_KEY = "master.environment.k8s.namespace";
  private static final String INSTANCE_LABEL = "master.environment.k8s.instance.label";
  private static final String POD_NAME_PATH = "master.environment.k8s.pod.name.path";
  private static final String POD_LABELS_PATH = "master.environment.k8s.pod.labels.path";
  private static final String POD_KILLER_SELECTOR = "master.environment.k8s.pod.killer.selector";
  private static final String POD_KILLER_DELAY_MILLIS = "master.environment.k8s.pod.killer.delay.millis";

  private static final String DEFAULT_NAMESPACE = "default";
  private static final String DEFAULT_INSTANCE_LABEL = "cdap.instance";
  private static final String DEFAULT_POD_NAME_PATH = "/etc/podinfo/pod.name";
  private static final String DEFAULT_POD_LABELS_PATH = "/etc/podinfo/pod.labels.properties";
  private static final String DEFAULT_POD_KILLER_SELECTOR = "cdap.container=preview";
  private static final long DEFAULT_POD_KILLER_DELAY_MILLIS = TimeUnit.HOURS.toMillis(1L);

  private static final Pattern LABEL_PATTERN = Pattern.compile("(cdap\\..+?)=\"(.*)\"");

  private KubeDiscoveryService discoveryService;
  private PodKillerTask podKillerTask;

  @Override
  public void initialize(MasterEnvironmentContext context) throws IOException {
    LOG.info("Initializing Kubernetes environment");

    Map<String, String> conf = context.getConfigurations();

    // Load the pod labels from the configured path. It should be setup by the CDAP operator
    Map<String, String> podLabels = new HashMap<>();
    String podLabelsPath = conf.getOrDefault(POD_LABELS_PATH, DEFAULT_POD_LABELS_PATH);
    try (BufferedReader reader = Files.newBufferedReader(new File(podLabelsPath).toPath(), StandardCharsets.UTF_8)) {
      String line = reader.readLine();
      while (line != null) {
        Matcher matcher = LABEL_PATTERN.matcher(line);
        if (matcher.matches()) {
          podLabels.put(matcher.group(1), matcher.group(2));
        }
        line = reader.readLine();
      }
    }

    String namespace = conf.getOrDefault(NAMESPACE_KEY, DEFAULT_NAMESPACE);

    // Get the instance label to setup prefix for K8s services
    String instanceLabel = conf.getOrDefault(INSTANCE_LABEL, DEFAULT_INSTANCE_LABEL);
    String instanceName = podLabels.get(instanceLabel);
    if (instanceName == null) {
      throw new IllegalStateException("Missing instance label '" + instanceLabel + "' from pod labels.");
    }

    // Get the OwnerReference. This is for setting the owner of K8s objects created by this environment
    List<V1OwnerReference> ownerReferences = getOwnerReference(conf.getOrDefault(POD_NAME_PATH, DEFAULT_POD_NAME_PATH),
                                                               namespace);

    // Services are publish to K8s with a prefix
    discoveryService = new KubeDiscoveryService(namespace, "cdap-" + instanceName + "-", podLabels, ownerReferences);

    // Optionally creates the pod killer task
    String podKillerSelector = conf.getOrDefault(POD_KILLER_SELECTOR, DEFAULT_POD_KILLER_SELECTOR);
    if (!Strings.isNullOrEmpty(podKillerSelector)) {
      long delayMillis = DEFAULT_POD_KILLER_DELAY_MILLIS;
      String confDelay = conf.get(POD_KILLER_DELAY_MILLIS);
      if (!Strings.isNullOrEmpty(confDelay)) {
        try {
          delayMillis = Long.parseLong(confDelay);
          if (delayMillis <= 0) {
            delayMillis = DEFAULT_POD_KILLER_DELAY_MILLIS;
            LOG.warn("Only positive value is allowed for configuration {}. Defaulting to ",
                     POD_KILLER_DELAY_MILLIS, delayMillis);
          }
        } catch (NumberFormatException e) {
          LOG.warn("Invalid value for configuration {}. Expected a positive integer, but get {}.",
                   POD_KILLER_DELAY_MILLIS, confDelay);
        }
      }

      podKillerTask = new PodKillerTask(namespace, podKillerSelector, delayMillis);
      LOG.info("Created pod killer task on namespace {}, with selector {} and delay {}",
               namespace, podKillerSelector, delayMillis);
    }

    LOG.info("Kubernetes environment initialized with pod labels {}", podLabels);
  }

  @Override
  public void destroy() {
    discoveryService.close();
    LOG.info("Kubernetes environment destroyed");
  }

  @Override
  public String getName() {
    return "k8s";
  }

  @Override
  public Supplier<DiscoveryService> getDiscoveryServiceSupplier() {
    return () -> discoveryService;
  }

  @Override
  public Supplier<DiscoveryServiceClient> getDiscoveryServiceClientSupplier() {
    return () -> discoveryService;
  }

  @Override
  public Optional<MasterEnvironmentTask> getTask() {
    return Optional.ofNullable(podKillerTask);
  }

  /**
   * Query the owner reference based on the instance label.
   */
  private List<V1OwnerReference> getOwnerReference(String podNamePath, String namespace) {
    try {
      String podName = Files.lines(Paths.get(podNamePath)).findFirst().orElse(null);
      if (podName == null) {
        LOG.warn("Failed to get pod name from path {}. No owner reference will be used.", podNamePath);
        return Collections.emptyList();
      }

      CoreV1Api api = new CoreV1Api(Config.defaultClient());
      V1Pod pod = api.readNamespacedPod(podName, namespace, null, null, null);
      return pod.getMetadata().getOwnerReferences();
    } catch (IOException e) {
      // If the CRD or CR is not available, just log and return null.
      // Missing owner reference won't affect functionality of CDAP.
      // It only affects K8s ability to garbage collect objects created by this service.
      LOG.warn("Failed to get the owner reference.", e);
    } catch (ApiException e) {
      LOG.warn("API error when fetching the owner reference. Code: {}, Message: {}",
               e.getCode(), e.getResponseBody(), e);
    }

    return Collections.emptyList();
  }
}

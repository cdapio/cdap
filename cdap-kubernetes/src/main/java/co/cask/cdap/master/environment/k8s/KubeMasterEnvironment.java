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
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of {@link MasterEnvironment} to provide the environment for running in Kubernetes.
 */
public class KubeMasterEnvironment implements MasterEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(KubeMasterEnvironment.class);

  private static final String NAMESPACE_KEY = "master.environment.k8s.namespace";
  private static final String POD_LABELS_PATH = "master.environment.k8s.pod.labels.path";

  private static final String DEFAULT_NAMESPACE = "default";
  private static final String DEFAULT_POD_LABELS_PATH = "/etc/podinfo/pod.labels.properties";

  private static final Pattern LABEL_PATTERN = Pattern.compile("(cdap\\..+?)=\"(.*)\"");

  private KubeDiscoveryService discoveryService;

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

    discoveryService = new KubeDiscoveryService(conf.getOrDefault(NAMESPACE_KEY, DEFAULT_NAMESPACE), podLabels);
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
}

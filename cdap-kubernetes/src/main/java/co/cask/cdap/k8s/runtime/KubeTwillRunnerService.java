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

package co.cask.cdap.k8s.runtime;

import co.cask.cdap.master.environment.k8s.PodInfo;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentList;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.util.Config;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Kubernetes version of a TwillRunner.
 *
 * All resources created by the runner will have a name of the form:
 *
 * [cleansed app name]-[runid]
 *
 * Each resource will also have the following labels:
 *
 * cdap.twill.runner=k8s
 * cdap.twill.run.id=[run id]
 * cdap.twill.app=[cleansed app name]
 *
 * and annotations:
 *
 * cdap.twill.app=[literal app name]
 */
public class KubeTwillRunnerService implements TwillRunnerService {

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillRunnerService.class);

  private static final String APP_ANNOTATION = "cdap.twill.app";
  private static final String APP_LABEL = "cdap.twill.app";
  private static final String RUNNER_LABEL = "cdap.twill.runner";
  private static final String RUNNER_LABEL_VAL = "k8s";
  private static final String RUN_ID_LABEL = "cdap.twill.run.id";
  private final String kubeNamespace;
  private final String resourcePrefix;
  private final PodInfo podInfo;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Map<String, String> extraLabels;
  private ApiClient apiClient;

  public KubeTwillRunnerService(String kubeNamespace, DiscoveryServiceClient discoveryServiceClient,
                                PodInfo podInfo, String resourcePrefix, Map<String, String> extraLabels) {
    this.kubeNamespace = kubeNamespace;
    this.podInfo = podInfo;
    this.resourcePrefix = resourcePrefix;
    this.discoveryServiceClient = discoveryServiceClient;
    this.extraLabels = Collections.unmodifiableMap(new HashMap<>(extraLabels));
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    TwillSpecification spec = application.configure();
    RunId runId = RunIds.generate();
    Map<String, String> labels = new HashMap<>(extraLabels);
    labels.put(RUNNER_LABEL, RUNNER_LABEL_VAL);
    labels.put(APP_LABEL, asLabel(spec.getName()));
    labels.put(RUN_ID_LABEL, runId.getId());
    V1ObjectMeta resourceMeta = new V1ObjectMeta();
    resourceMeta.setOwnerReferences(podInfo.getOwnerReferences());
    resourceMeta.setName(getName(spec.getName(), runId));
    resourceMeta.setLabels(labels);
    // labels have more strict requirements around valid character sets,
    // so use annotations to store the app name.
    resourceMeta.setAnnotations(Collections.singletonMap(APP_ANNOTATION, spec.getName()));
    return new KubeTwillPreparer(apiClient, kubeNamespace, podInfo,
                                 runId, spec, resourceMeta, discoveryServiceClient);
  }

  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    try {
      AppsV1Api appsApi = new AppsV1Api(apiClient);
      String name = getName(applicationName, runId);
      V1Deployment deployment = appsApi.readNamespacedDeployment(name, kubeNamespace, null, null, null);
      if (deployment == null) {
        return null;
      }

      return new KubeTwillController(name, kubeNamespace, runId, apiClient, discoveryServiceClient);
    } catch (ApiException e) {
      if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        return null;
      }
      throw new RuntimeException("Exception while reading deployment for " + applicationName, e);
    }
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    Map<String, String> labels = new HashMap<>();
    labels.put(RUNNER_LABEL, RUNNER_LABEL_VAL);
    labels.put(APP_LABEL, asLabel(applicationName));

    List<TwillController> results = new ArrayList<>();
    list(labels, deployment -> {
      String runIdStr = deployment.getMetadata().getLabels().get(RUN_ID_LABEL);
      RunId runId = RunIds.fromString(runIdStr);
      results.add(new KubeTwillController(deployment.getMetadata().getName(), kubeNamespace, runId, apiClient,
                                          discoveryServiceClient));
    });
    return results;
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    Map<String, String> labels = new HashMap<>();
    labels.put(RUNNER_LABEL, RUNNER_LABEL_VAL);

    List<LiveInfo> results = new ArrayList<>();
    list(labels, deployment -> {
      String appName = deployment.getMetadata().getAnnotations().get(APP_ANNOTATION);
      String runIdStr = deployment.getMetadata().getLabels().get(RUN_ID_LABEL);
      RunId runId = RunIds.fromString(runIdStr);

      TwillController controller = new KubeTwillController(deployment.getMetadata().getName(), kubeNamespace,
                                                           runId, apiClient, discoveryServiceClient);
      LiveInfo liveInfo = new KubeLiveInfo(appName, Collections.singleton(controller));
      results.add(liveInfo);
    });
    return results;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay, long delay,
                                               TimeUnit unit) {
    return () -> { };
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay, long delay, long retryDelay,
                                           TimeUnit unit) {
    return () -> { };
  }

  private void list(Map<String, String> labelSelector, Consumer<V1Deployment> consumer) {
    String labels = labelSelector.entrySet().stream()
      .map(entry -> entry.getKey() + "=" + entry.getValue())
      .collect(Collectors.joining(","));
    try {
      AppsV1Api appsApi = new AppsV1Api(apiClient);
      V1DeploymentList deployments = appsApi.listNamespacedDeployment(kubeNamespace, null, null, null, null,
                                                                      labels, null, null, null, null);

      for (V1Deployment deployment : deployments.getItems()) {
        Map<String, String> deploymentLabels = deployment.getMetadata().getLabels();
        String runnerVal = deploymentLabels.get(RUNNER_LABEL);
        if (!RUNNER_LABEL_VAL.equals(runnerVal)) {
          continue;
        }
        consumer.accept(deployment);
      }
    } catch (ApiException e) {
      throw new RuntimeException("Exception while listing deployments in Kubernetes", e);
    }
  }

  @Override
  public void start() {
    try {
      apiClient = Config.defaultClient();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to get Kubernetes API Client", e);
    }
  }

  @Override
  public void stop() {

  }

  /**
   * A TwillApplication with a single runnable.
   */
  private static class SingleRunnableApplication implements TwillApplication {

    private final TwillRunnable runnable;
    private final ResourceSpecification resourceSpec;

    SingleRunnableApplication(TwillRunnable runnable, ResourceSpecification resourceSpec) {
      this.runnable = runnable;
      this.resourceSpec = resourceSpec;
    }

    @Override
    public TwillSpecification configure() {
      TwillRunnableSpecification runnableSpec = runnable.configure();
      return TwillSpecification.Builder.with()
        .setName(runnableSpec.getName())
        .withRunnable().add(runnableSpec.getName(), runnable, resourceSpec)
        .noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  /**
   * Kubernetes names can be at max 253 characters and only contain lowercase alphanumeric, '-', and '.'.
   * This will lowercase the twill name, and append -[runid] to the end. If the result would be longer than 253
   * characters, the twill name is truncated.
   */
  private String getName(String applicationName, RunId runId) {
    String suffix = "-" + runId.getId();
    return resourcePrefix + cleanse(applicationName, 253 - suffix.length() - resourcePrefix.length()) + suffix;
  }

  /**
   * label values must be 63 characters or less and consist of alphanumeric, '.', '_', or '-'.
   */
  private String asLabel(String val) {
    return cleanse(val, 63);
  }

  /**
   * Kubernetes names must be lower case alphanumeric, or '-'. Some are less restrictive, but those characters
   * should always be ok.
   */
  private String cleanse(String val, int maxLength) {
    String cleansed = val.replaceAll("[^A-Za-z0-9\\-]", "-");
    return cleansed.length() > maxLength ? cleansed.substring(0, maxLength) : cleansed;
  }

  /**
   * Kubernetes LiveInfo.
   */
  private static class KubeLiveInfo implements LiveInfo {
    private final String applicationName;
    private final Iterable<TwillController> controllers;

    KubeLiveInfo(String applicationName, Iterable<TwillController> controllers) {
      this.applicationName = applicationName;
      this.controllers = controllers;
    }

    @Override
    public String getApplicationName() {
      return applicationName;
    }

    @Override
    public Iterable<TwillController> getControllers() {
      return controllers;
    }
  }
}

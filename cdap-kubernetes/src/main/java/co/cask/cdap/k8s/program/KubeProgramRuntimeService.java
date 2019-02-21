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

package co.cask.cdap.k8s.program;

import co.cask.cdap.master.spi.program.ProgramController;
import co.cask.cdap.master.spi.program.ProgramDescriptor;
import co.cask.cdap.master.spi.program.ProgramOptions;
import co.cask.cdap.master.spi.program.ProgramRuntimeService;
import co.cask.cdap.master.spi.program.RuntimeInfo;
import co.cask.cdap.master.spi.program.SerDe;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentList;
import io.kubernetes.client.models.V1DeploymentSpec;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import io.kubernetes.client.util.Config;
import org.apache.twill.api.RunId;
import org.apache.twill.internal.RunIds;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Kubernetes program runtime service.
 */
public class KubeProgramRuntimeService implements ProgramRuntimeService {
  private static final String APP_SPEC = "appSpec";
  private static final String PROGRAM_OPTIONS = "programOptions";
  private static final String NAMESPACE_LABEL = "cdap-namespace";
  private static final String APP_LABEL = "cdap-app";
  private static final String PROGRAM_TYPE_LABEL = "cdap-program-type";
  private static final String PROGRAM_LABEL = "cdap-program";
  private static final String RUN_LABEL = "cdap-run";
  private final String kubeNamespace;
  private final String image;
  private final String classpath;
  private final SerDe serDe;
  private ApiClient apiClient;

  public KubeProgramRuntimeService(String kubeNamespace, String image, String classpath, SerDe serDe) {
    this.kubeNamespace = kubeNamespace;
    this.image = image;
    this.serDe = serDe;
    // this should be hardcoded once the image is ready.
    this.classpath = classpath;
  }

  /**
   * Programs require files for the application spec and program options to be passed to them as command line args.
   * This method creates a config-map for the app spec and program options, then creates a deployment that has
   * the config map mounted in a volume.
   *
   * Pods created by this method will be labeled with 'cdap-namespace', 'cdap-app', 'cdap-program-type', 'cdap-program',
   * and 'cdap-run'.
   */
  @Override
  public RuntimeInfo run(ProgramDescriptor programDescriptor, ProgramOptions options, RunId runId) {
    if (ProgramType.SERVICE != programDescriptor.getProgramId().getType()) {
      throw new IllegalArgumentException("Only Service programs are currently allowed to run in Kubernetes");
    }

    ProgramRunId programRunId = programDescriptor.getProgramId().run(runId);
    try {
      V1ConfigMap configMap = createConfigMap(programDescriptor, options, programRunId);
      createDeployment(programDescriptor, programRunId, configMap);
      ProgramController programController = new KubeProgramController(programRunId, kubeNamespace, getApiClient());
      return new KubeRuntimeInfo(programController);
    } catch (ApiException e) {
      throw new RuntimeException(
        String.format("Exception interacting with Kubernetes while executing %s", programRunId), e);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Exception executing %s in Kubernetes", programRunId), e);
    }
  }

  private V1Deployment createDeployment(ProgramDescriptor programDescriptor, ProgramRunId programRunId,
                                        V1ConfigMap configMap) throws IOException, ApiException {

    /*
       The deployment will look something like:

       apiVersion: apps/v1
       kind: Deployment
       metadata:
         [deployment meta]
       spec:
         [deployment spec]
       template:
         [template spec]
     */
    AppsV1Api appsApi = new AppsV1Api(getApiClient());
    V1Deployment deployment = new V1Deployment();

    Map<String, String> labels = getLabels(programRunId);

    /*
       metadata:
         name: service-runid
         labels:
           app: cdap
           cdap-namespace: system
           cdap-app: dataprep
           cdap-program-type: SERVICE
           cdap-program: service
           cdap-run: runid
     */
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(KubernetesPrograms.getResourceName(programRunId));
    meta.setLabels(labels);
    deployment.setMetadata(meta);

    /*
       spec:
         replicas: 1
         selector:
           matchLabels:
             app: cdap
             cdap-namespace: system
             cdap-app: dataprep
             cdap-program-type: SERVICE
             cdap-program: service
             cdap-run: runid
     */
    V1DeploymentSpec spec = new V1DeploymentSpec();
    V1LabelSelector labelSelector = new V1LabelSelector();
    labelSelector.matchLabels(labels);
    spec.setSelector(labelSelector);
    // TODO: figure out scaling
    spec.setReplicas(1);

    /*
       template:
         metadata:
           labels:
             app: cdap
             cdap-namespace: system
             cdap-app: dataprep
             cdap-program-type: SERVICE
             cdap-program: service
             cdap-run: runid
         spec:
           [pod spec]
     */
    V1PodTemplateSpec templateSpec = new V1PodTemplateSpec();
    V1ObjectMeta templateMeta = new V1ObjectMeta();
    templateMeta.setLabels(labels);
    templateSpec.setMetadata(templateMeta);

    /*
         spec:
           containers:
           - name: cdap-k8s
             image: gcr.io/cdap-dogfood/cdap-sandbox:k8s-26
             command: ["java"]
             args: ["-cp", "classpath", "co.cask.cdap.internal.app.runtime.k8s.UserServiceProgramMain", "--env=k8s",
                    "--appSpecPath=/etc/podinfo/appSpec", "--programOptions=/etc/podinfo/programOptions"]
             volumeMounts:
             - name: config-volume
               mountPath: /etc/podinfo
           volumes:
           - name: config-volume
             configMap:
               name: service-runid
     */
    V1PodSpec podSpec = new V1PodSpec();

    String volumeName = "config-volume";
    V1Volume volume = new V1Volume();
    volume.setName(volumeName);
    V1ConfigMapVolumeSource configMapVolumeSource = new V1ConfigMapVolumeSource();
    configMapVolumeSource.setName(configMap.getMetadata().getName());
    volume.setConfigMap(configMapVolumeSource);
    podSpec.setVolumes(Collections.singletonList(volume));

    V1Container container = new V1Container();
    container.setName("cdap-k8s");
    container.setImage(image);

    String mountPath = "/etc/podinfo";
    V1VolumeMount volumeMount = new V1VolumeMount();
    volumeMount.setName(volumeName);
    volumeMount.setMountPath(mountPath);
    container.setVolumeMounts(Collections.singletonList(volumeMount));

    container.setCommand(Collections.singletonList("java"));
    // when other program types are supported, there will need to be a mapping from program type to main class
    container.setArgs(Arrays.asList("-cp", classpath, "co.cask.cdap.internal.app.runtime.k8s.UserServiceProgramMain",
                                    "--env=k8s", String.format("--appSpecPath=%s/%s", mountPath, APP_SPEC),
                                    String.format("--programOptions=%s/%s", mountPath, PROGRAM_OPTIONS)));
    podSpec.setContainers(Collections.singletonList(container));

    templateSpec.setSpec(podSpec);
    spec.setTemplate(templateSpec);
    deployment.setSpec(spec);

    return appsApi.createNamespacedDeployment(kubeNamespace, deployment, "true");
  }

  /**
   * Create the config map for the program run. Since the config map contains the program options, which contains
   * runtime arguments, it needs to be created for each program run and deleted when the run ends.
   */
  private V1ConfigMap createConfigMap(ProgramDescriptor programDescriptor, ProgramOptions options,
                                      ProgramRunId programRunId) throws ApiException, IOException {
    CoreV1Api api = new CoreV1Api(getApiClient());
    V1ConfigMap configMap = new V1ConfigMap();
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(KubernetesPrograms.getResourceName(programRunId));
    meta.setLabels(getLabels(programRunId));
    configMap.setMetadata(meta);
    configMap.putBinaryDataItem(APP_SPEC, serDe.serialize(programDescriptor.getApplicationSpecification()));
    configMap.putBinaryDataItem(PROGRAM_OPTIONS, serDe.serialize(options));
    return api.createNamespacedConfigMap(kubeNamespace, configMap, "true");
  }

  @Nullable
  @Override
  public RuntimeInfo lookup(ProgramId programId, RunId runId) {
    ProgramRunId programRunId = programId.run(runId);
    try {
      ApiClient apiClient = getApiClient();
      AppsV1Api appsApi = new AppsV1Api(apiClient);
      V1Deployment deployment = appsApi.readNamespacedDeployment(KubernetesPrograms.getResourceName(programRunId),
                                                                 kubeNamespace, null, null, null);

      ProgramController programController = new KubeProgramController(programRunId, kubeNamespace, apiClient);
      return new KubeRuntimeInfo(programController);
    } catch (ApiException e) {
      if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        return null;
      }
      throw new RuntimeException("Exception while reading deployment for program run " + programRunId, e);
    } catch (IOException e) {
      throw new RuntimeException("Exception while reading deployment for program run " + programRunId, e);
    }
  }

  @Override
  public Map<RunId, RuntimeInfo> list(ProgramType type) {
    Map<String, String> labels = new HashMap<>();
    labels.put("app", "cdap");
    labels.put(PROGRAM_TYPE_LABEL, type.name());
    return list(labels);
  }

  @Override
  public Map<RunId, RuntimeInfo> list(ProgramId program) {
    Map<String, String> labels = new HashMap<>();
    labels.put("app", "cdap");
    labels.put(NAMESPACE_LABEL, program.getNamespace());
    labels.put(APP_LABEL, program.getApplication());
    labels.put(PROGRAM_LABEL, program.getProgram());
    labels.put(PROGRAM_TYPE_LABEL, program.getType().name());
    return list(labels);
  }

  @Override
  public ProgramLiveInfo getLiveInfo(ProgramId programId) {
    return new ProgramLiveInfo(programId, "kubernetes") { };
  }

  private Map<RunId, RuntimeInfo> list(Map<String, String> labelSelector) {
    String labels = labelSelector.entrySet().stream()
      .map(entry -> entry.getKey() + "=" + entry.getValue())
      .collect(Collectors.joining(","));
    try {
      ApiClient apiClient = getApiClient();
      AppsV1Api appsApi = new AppsV1Api(apiClient);
      V1DeploymentList deployments = appsApi.listNamespacedDeployment(kubeNamespace, null, null, null, null,
                                                                      labels, null, null, null, null);

      Map<RunId, RuntimeInfo> result = new HashMap<>();
      for (V1Deployment deployment : deployments.getItems()) {
        Map<String, String> deploymentLabels = deployment.getMetadata().getLabels();
        String namespace = deploymentLabels.get(NAMESPACE_LABEL);
        String app = deploymentLabels.get(APP_LABEL);
        ProgramType programType = ProgramType.valueOf(deploymentLabels.get(PROGRAM_TYPE_LABEL));
        String program = deploymentLabels.get(PROGRAM_LABEL);
        RunId runId = RunIds.fromString(deploymentLabels.get(RUN_LABEL));
        ProgramRunId programRunId = new NamespaceId(namespace).app(app).program(programType, program).run(runId);
        ProgramController programController = new KubeProgramController(programRunId, kubeNamespace, apiClient);
        result.put(runId, new KubeRuntimeInfo(programController));
      }
      return result;
    } catch (ApiException | IOException e) {
      throw new RuntimeException("Exception while listing deployments in Kubernetes", e);
    }
  }

  private Map<String, String> getLabels(ProgramRunId programRunId) {
    Map<String, String> labels = new HashMap<>();
    labels.put("app", "cdap");
    labels.put(NAMESPACE_LABEL, programRunId.getNamespace());
    labels.put(APP_LABEL, programRunId.getApplication());
    labels.put(PROGRAM_TYPE_LABEL, programRunId.getType().name());
    labels.put(PROGRAM_LABEL, programRunId.getProgram());
    labels.put(RUN_LABEL, programRunId.getRun());
    return labels;
  }

  private ApiClient getApiClient() throws IOException {
    ApiClient api = apiClient;
    if (api != null) {
      return api;
    }

    synchronized (this) {
      api = apiClient;
      if (api != null) {
        return api;
      }

      apiClient = Config.defaultClient();
      return apiClient;
    }
  }
}

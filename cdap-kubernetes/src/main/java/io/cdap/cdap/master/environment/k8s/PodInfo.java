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

package io.cdap.cdap.master.environment.k8s;

import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1OwnerReference;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Pod information.
 */
public final class PodInfo {

  private final String apiVersion;
  private final String kind;
  private final String name;
  private final String uid;
  private final String podInfoDir;
  private final String labelsFile;
  private final String nameFile;
  private final String namespace;
  private final Map<String, String> labels;
  private final List<V1OwnerReference> ownerReferences;
  private final String serviceAccountName;
  private final String runtimeClassName;
  private final List<V1Volume> volumes;
  private final String containerLabelName;
  private final String containerImage;
  private final List<V1VolumeMount> containerVolumeMounts;
  private final List<V1EnvVar> containerEnvironments;

  public PodInfo(String apiVersion, String kind, String name, String uid, String podInfoDir, String labelsFile,
                 String nameFile, String namespace, Map<String, String> labels, List<V1OwnerReference> ownerReferences,
                 String serviceAccountName, String runtimeClassName, List<V1Volume> volumes, String containerLabelName,
                 String containerImage, List<V1VolumeMount> containerVolumeMounts,
                 List<V1EnvVar> containerEnvironments) {
    this.apiVersion = apiVersion;
    this.kind = kind;
    this.name = name;
    this.uid = uid;
    this.podInfoDir = podInfoDir;
    this.labelsFile = labelsFile;
    this.nameFile = nameFile;
    this.namespace = namespace;
    this.labels = labels;
    this.ownerReferences = Collections.unmodifiableList(new ArrayList<>(ownerReferences));
    this.serviceAccountName = serviceAccountName;
    this.runtimeClassName = runtimeClassName;
    this.volumes = Collections.unmodifiableList(new ArrayList<>(volumes));
    this.containerLabelName = containerLabelName;
    this.containerImage = containerImage;
    this.containerVolumeMounts = Collections.unmodifiableList(new ArrayList<>(containerVolumeMounts));
    this.containerEnvironments = Collections.unmodifiableList(new ArrayList<>(containerEnvironments));
  }

  public String getPodInfoDir() {
    return podInfoDir;
  }

  public String getLabelsFile() {
    return labelsFile;
  }

  public String getNameFile() {
    return nameFile;
  }

  public String getNamespace() {
    return namespace;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public List<V1OwnerReference> getOwnerReferences() {
    return ownerReferences;
  }

  public String getServiceAccountName() {
    return serviceAccountName;
  }

  public String getRuntimeClassName() {
    return runtimeClassName;
  }

  public List<V1Volume> getVolumes() {
    return volumes;
  }

  /**
   * Returns the label name that used to hold the name of the container that runs CDAP.
   */
  public String getContainerLabelName() {
    return containerLabelName;
  }

  public String getContainerImage() {
    return containerImage;
  }

  public List<V1VolumeMount> getContainerVolumeMounts() {
    return containerVolumeMounts;
  }

  public List<V1EnvVar> getContainerEnvironments() {
    return containerEnvironments;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public String getKind() {
    return kind;
  }

  public String getName() {
    return name;
  }

  public String getUid() {
    return uid;
  }
}

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

import io.kubernetes.client.models.V1OwnerReference;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Pod information
 */
public class PodInfo {
  private final String podInfoDir;
  private final String labelsFile;
  private final String nameFile;
  private final String namespace;
  private final Map<String, String> labels;
  private final List<V1OwnerReference> ownerReferences;

  public PodInfo(String podInfoDir, String labelsFile, String nameFile, String namespace,
                 Map<String, String> labels, List<V1OwnerReference> ownerReferences) {
    this.podInfoDir = podInfoDir;
    this.labelsFile = labelsFile;
    this.nameFile = nameFile;
    this.namespace = namespace;
    this.labels = labels;
    this.ownerReferences = ownerReferences;
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
}

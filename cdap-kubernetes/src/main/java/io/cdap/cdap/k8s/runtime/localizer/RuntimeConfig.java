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

package io.cdap.cdap.k8s.runtime.localizer;

import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.net.URI;
import java.util.Collection;

/**
 * Holds information about the runtime configs that are created at prepare time and must be available at launch time
 * in the FileLocalizer.
 */
public class RuntimeConfig {
  private final URI uri;
  private final Collection<V1Volume> volumes;
  private final Collection<V1VolumeMount> mounts;

  public RuntimeConfig(URI uri, Collection<V1Volume> volumes, Collection<V1VolumeMount> mounts) {
    this.uri = uri;
    this.volumes = volumes;
    this.mounts = mounts;
  }

  /**
   * @return URI for the runtime config jar, containing the twill spec and app and runnable arguments.
   */
  public URI getUri() {
    return uri;
  }

  /**
   * @return all volumes that should be added to the pods
   */
  public Collection<V1Volume> getVolumes() {
    return volumes;
  }

  /**
   * @return all volume mount that should be added to all pod containers
   */
  public Collection<V1VolumeMount> getVolumeMounts() {
    return mounts;
  }
}

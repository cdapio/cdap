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

import com.google.common.io.Resources;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1KeyToPath;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.DefaultLocalFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;

/**
 * Loads config files into a config map that is then mounted onto the pods and available locally.
 */
public class ConfigMapPreparer extends FilePreparer {
  private static final String MOUNT_DIR = "/etc/runtimeconfig";
  private final String namespace;
  private final String configMapName;
  private final CoreV1Api coreV1Api;
  private final V1ConfigMapBuilder configMapBuilder;
  private final V1ConfigMapVolumeSourceBuilder volumeSourceBuilder;
  private final LocationPreparer locationPreparer;

  public ConfigMapPreparer(RunId runId, PodInfo podInfo, ApiClient apiClient, Location appLocation) {
    this.configMapName = "runtime-config-" + runId.getId();
    this.configMapBuilder = new V1ConfigMapBuilder().withMetadata(
      new V1ObjectMetaBuilder()
        .withName(configMapName)
        .withLabels(podInfo.getLabels())
        .withOwnerReferences(podInfo.getOwnerReferences())
        .build());
    this.coreV1Api = new CoreV1Api(apiClient);
    this.namespace = podInfo.getNamespace();
    this.volumeSourceBuilder = new V1ConfigMapVolumeSourceBuilder().withName(configMapName);
    this.locationPreparer = new LocationPreparer(appLocation, "cdap");
  }

  /**
   * Adds each non-jar local file to the config map as a separate key.
   * The key in the config map is [runnableName]-[file name].
   * Jar files will have their URI changed to use the "cdap" scheme, which will tell the
   * FileLocalizer to pull it from app-fabric.
   */
  @Override
  public LocalFile prepareFile(String runnableName, LocalFile localFile) throws IOException {
    // jars are too big to put in the config map and should be accessible from app-fabric
    // modify the scheme to be "cdap". This is needed in case CDAP is configured to use local storage so
    // the scheme is "file". This way the FileLocalizer knows to pull it and it is not a local file in the container.
    if (localFile.getName().endsWith(".jar") || localFile.isArchive()) {
      return locationPreparer.prepareFile(runnableName, localFile);
    }

    String filePath = String.format("%s/%s", runnableName, localFile.getName());
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      Resources.copy(localFile.getURI().toURL(), os);
      String key = String.format("%s-%s", runnableName, localFile.getName());
      configMapBuilder.addToBinaryData(key, os.toByteArray());
      volumeSourceBuilder.addToItems(new V1KeyToPath().key(key).path(filePath));
    }
    URI localURI = URI.create(String.format("file://%s/%s", MOUNT_DIR, filePath));
    return new DefaultLocalFile(localFile.getName(), localURI, localFile.getLastModified(),
                                localFile.getSize(), localFile.isArchive(), localFile.getPattern());
  }

  @Override
  public RuntimeConfig prepareRuntimeConfig(Path configDir) throws IOException {
    String jarName = "runtime-config.jar";
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      writeJar(configDir, os);
      configMapBuilder.addToBinaryData(jarName, os.toByteArray());
      volumeSourceBuilder.addToItems(new V1KeyToPath().key(jarName).path(jarName));
    }

    V1ConfigMap configMap = configMapBuilder.build();
    try {
      coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null);
    } catch (ApiException e) {
      throw new IOException("Unable to create config map to localize files: " + e.getResponseBody(), e);
    }

    /*
        volume here will be something like:

        volumes:
          - name: runtime-config
            configMap:
              name: runtime-config-[runid]
              items:
              - key: cConf.xml
                path: [runnable name]/cConf.xml
              - key: runtime-config.jar
                path: runtime-config.jar

        while the volumeMount will be something like:

        volumeMounts:
        - name: runtime-config
          mountPath: /etc/runtimeconfig
     */
    V1Volume volume = new V1Volume().name("runtime-config").configMap(volumeSourceBuilder.build());
    V1VolumeMount mount = new V1VolumeMount().name("runtime-config").mountPath(MOUNT_DIR).readOnly(true);
    URI jarURI = URI.create(String.format("file://%s/%s", MOUNT_DIR, jarName));
    return new RuntimeConfig(jarURI, Collections.singleton(volume), Collections.singleton(mount));
  }

}

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

package io.cdap.cdap.internal.capability.autoinstall;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.artifact.InvalidArtifactRangeException;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.capability.HttpClients;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.artifact.ArtifactRanges;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a package json in the hub.
 */
public class HubPackage {
  private static final Logger LOG = LoggerFactory.getLogger(HubPackage.class);
  private static final Gson GSON = new Gson();

  private final String name;
  private final String version;
  private final String description;
  private final String label;
  private final String author;
  private final String org;
  private final String cdapVersion;
  private final long created;
  private final boolean beta;
  private final List<String> categories;
  private final boolean paid;
  private final String paidLink;
  private transient ArtifactVersionRange versionRange;
  private transient ArtifactVersion artifactVersion;

  public HubPackage(String name, String version, String description, String label, String author, String org,
                    String cdapVersion, long created, boolean beta, List<String> categories,
                    boolean paid, String paidLink) throws InvalidArtifactRangeException {
    this.name = name;
    this.version = version;
    this.artifactVersion = new ArtifactVersion(version);
    this.description = description;
    this.label = label;
    this.author = author;
    this.org = org;
    this.cdapVersion = cdapVersion;
    this.versionRange = ArtifactVersionRange.parse(cdapVersion);
    this.created = created;
    this.beta = beta;
    this.categories = categories;
    this.paid = paid;
    this.paidLink = paidLink;
  }

  private String getCdapVersion() {
    return cdapVersion;
  }

  public ArtifactVersion getArtifactVersion() {
    if (artifactVersion == null) {
      // artifactVersion will be null if the class was deserialized from json.
      artifactVersion = new ArtifactVersion(version);

    }
    return artifactVersion;
  }

  public boolean versionIsInRange(String version) {
    if (versionRange == null) {
      // versionRange will be null if the class was deserialized from json.
      if (cdapVersion == null) {
        return false;
      }
      try {
        versionRange = ArtifactVersionRange.parse(cdapVersion);
      } catch (InvalidArtifactRangeException e) {
        LOG.warn(String.format("%s has invalid artifact range", name), e);
        return false;
      }
    }
    return versionRange.versionIsInRange(new ArtifactVersion(version));
  }

  /**
   * Downloads the plugin from the given URL and installs it in the artifact repository
   *
   * @param url URL that points to the plugin directory on the hub
   * @param artifactRepository {@link ArtifactRepository} in which the plugin will be installed
   * @param tmpDir temporary directory where plugin jar is downloaded from the hub
   */
  public void installPlugin(URL url, ArtifactRepository artifactRepository, File tmpDir) throws Exception {
    // Deserialize spec.json
    URL specURL = new URL(url, "/v2/packages/" + name + "/" + version + "/spec.json");
    Spec spec = GSON.fromJson(HttpClients.doGetAsString(specURL), Spec.class);
    for (Spec.Action action: spec.getActions()) {
      // Get plugin jar and json names under one_step_deploy_plugin action in the spec
      // See https://cdap.atlassian.net/wiki/spaces/DOCS/pages/554401840/Hub+API?src=search#one_step_deploy_plugin
      if (!action.getType().equals("one_step_deploy_plugin")) {
        continue;
      }

      String configFilename = action.getConfigFilename();
      if (configFilename == null) {
        LOG.warn("Ignoring plugin {} due to missing config", name);
        continue;
      }
      URL configURL = new URL(url, Joiner.on("/").join(Arrays.asList("/v2/packages", name, version, configFilename)));
      // Download plugin json from hub
      JsonObject jsonObj = GSON.fromJson(HttpClients.doGetAsString(configURL), JsonObject.class);
      List<String> parents = GSON.fromJson(jsonObj.get("parents"), new TypeToken<List<String>>() {
      }.getType());

      String jarName = action.getJarName();
      if (jarName == null) {
        LOG.warn("Ignoring plugin {} due to missing jar", name);
        continue;
      }
      // Download plugin jar from hub
      File destination = File.createTempFile("artifact-", ".jar", tmpDir);
      FileChannel channel = new FileOutputStream(destination, false).getChannel();
      URL jarURL = new URL(url, Joiner.on("/").join(Arrays.asList("/v2/packages", name, version, jarName)));
      HttpRequest request = HttpRequest.get(jarURL).withContentConsumer(new HttpContentConsumer() {
        @Override
        public boolean onReceived(ByteBuffer buffer) {
          try {
            channel.write(buffer);
          } catch (IOException e) {
            LOG.error("Failed write to file {}", destination);
            return false;
          }
          return true;
        }

        @Override
        public void onFinished() {
          Closeables.closeQuietly(channel);
        }
      }).build();
      HttpClients.executeStreamingRequest(request);

      Set<ArtifactRange> parentArtifacts = new HashSet<>();
      for (String parent : parents) {
        try {
          // try parsing it as a namespaced range like system:cdap-data-pipeline[6.3 1.1,7.0.0)
          parentArtifacts.add(ArtifactRanges.parseArtifactRange(parent));
        } catch (InvalidArtifactRangeException e) {
          // if this failed, try parsing as a non-namespaced range like cdap-data-pipeline[6.3 1.1,7.0.0)
          parentArtifacts.add(ArtifactRanges.parseArtifactRange(NamespaceId.DEFAULT.getNamespace(), parent));
        }
      }

      // add the artifact to the repo
      io.cdap.cdap.proto.id.ArtifactId artifactId = NamespaceId.DEFAULT.artifact(name, version);
      Id.Artifact artifact = Id.Artifact.fromEntityId(artifactId);
      try {
        artifactRepository.addArtifact(artifact,
                                       destination, parentArtifacts, ImmutableSet.of());
      } catch (ArtifactAlreadyExistsException e) {
        LOG.debug("Artifact artifact {}-{} already exists", name, version);
      }

      Map<String, String> properties = GSON.fromJson(jsonObj.get("properties"), new TypeToken<Map<String, String>>() {
      }.getType());
      artifactRepository.writeArtifactProperties(Id.Artifact.fromEntityId(artifactId), properties);

      if (!java.nio.file.Files.deleteIfExists(Paths.get(destination.getPath()))) {
        LOG.warn("Failed to cleanup file {}", destination);
      }
    }
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getDescription() {
    return description;
  }

  public String getLabel() {
    return label;
  }

  public String getAuthor() {
    return author;
  }

  public String getOrg() {
    return org;
  }

  public long getCreated() {
    return created;
  }

  public boolean getBeta() {
    return beta;
  }

  public List<String> getCategories() {
    return categories;
  }

  public boolean getPaid() {
    return paid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HubPackage that = (HubPackage) o;
    return Objects.equals(name, that.name)
      && Objects.equals(version, that.version)
      && Objects.equals(description, that.description)
      && Objects.equals(label, that.label)
      && Objects.equals(author, that.author)
      && Objects.equals(org, that.org)
      && Objects.equals(cdapVersion, that.cdapVersion)
      && Objects.equals(created, that.created)
      && Objects.equals(beta, that.beta)
      && Objects.equals(categories, that.categories)
      && Objects.equals(paid, that.paid)
      && Objects.equals(paidLink, that.paidLink);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version, description, label, author, org, cdapVersion,
                        created, beta, categories, paid, paidLink);
  }
}

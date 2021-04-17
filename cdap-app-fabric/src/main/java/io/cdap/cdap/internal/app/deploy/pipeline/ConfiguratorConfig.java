/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.deploy.pipeline;

import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;

import java.net.URI;
import javax.annotation.Nullable;

/**
 * ConfiguratorConfig encapsulates the config that {@link io.cdap.cdap.internal.app.deploy.RemoteConfigurator} sends
 * over to a {@link io.cdap.cdap.app.deploy.Configurator} running remotely.
 */
public class ConfiguratorConfig {
  private final String applicationName;
  private final String applicationVersion;
  private final String configString;
  // this is the namespace that the app will be in, which may be different than the namespace of the artifact.
  // if the artifact is a system artifact, the namespace will be the system namespace.
  private final NamespaceId appNamespace;

  private final String appClassName;
  private final ArtifactId artifactId;
  private final URI artifactLocationURI;

  public ConfiguratorConfig(AppDeploymentInfo deploymentInfo) {
    this.artifactLocationURI = deploymentInfo.getArtifactLocation().toURI();
    this.appNamespace = deploymentInfo.getNamespaceId();
    this.artifactId = deploymentInfo.getArtifactId();
    this.appClassName = deploymentInfo.getApplicationClass().getClassName();
    this.applicationName = deploymentInfo.getApplicationName();
    this.applicationVersion = deploymentInfo.getApplicationVersion();
    this.configString = deploymentInfo.getConfigString() == null ? "" : deploymentInfo.getConfigString();
  }

  public ConfiguratorConfig(NamespaceId appNamespace, ArtifactId artifactId,
                            String appClassName,
                            @Nullable String applicationName, @Nullable String applicationVersion,
                            @Nullable String configString, URI artifactLocation) {
    this.artifactLocationURI = artifactLocation;
    this.appNamespace = appNamespace;
    this.artifactId = artifactId;
    this.appClassName = appClassName;
    this.applicationName = applicationName;
    this.applicationVersion = applicationVersion;
    this.configString = configString == null ? "" : configString;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getApplicationVersion() {
    return applicationVersion;
  }

  public String getConfigString() {
    return configString;
  }

  public NamespaceId getAppNamespace() {
    return appNamespace;
  }

  public String getAppClassName() {
    return appClassName;
  }

  public ArtifactId getArtifactId() {
    return artifactId;
  }

  public URI getArtifactLocationURI() {
    return artifactLocationURI;
  }
}

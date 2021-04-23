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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;

/**
 * ConfiguratorConfig encapsulates the config that {@link io.cdap.cdap.internal.app.deploy.RemoteConfigurator}
 * sends over to a {@link io.cdap.cdap.app.deploy.Configurator} running remotely.
 */
public class ConfiguratorConfig {
  private final String applicationName;
  private final String applicationVersion;
  private final String configString;
  // this is the namespace that the app will be in, which may be different than the namespace of the artifact.
  // if the artifact is a system artifact, the namespace will be the system namespace.
  private final Id.Namespace appNamespace;

  private final String appClassName;
  private final Id.Artifact artifactId;
  private final String cConf;
  private final URI artifactLocationURI;

  public ConfiguratorConfig(CConfiguration cConf, Id.Namespace appNamespace, Id.Artifact artifactId,
                            String appClassName,
                            @Nullable String applicationName, @Nullable String applicationVersion,
                            @Nullable String configString, URI artifactLocation) {
    this.artifactLocationURI = artifactLocation;
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    try {
      cConf.writeXml(output);

    } catch (IOException e) {
      e.printStackTrace();

    }
    this.cConf = new String(output.toByteArray());
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

  public Id.Namespace getAppNamespace() {
    return appNamespace;
  }

  public String getAppClassName() {
    return appClassName;
  }

  public Id.Artifact getArtifactId() {
    return artifactId;
  }

  public CConfiguration getcConf() {
    return CConfiguration.create(new ByteArrayInputStream(cConf.getBytes()));
  }

  public URI getArtifactLocationURI() {
    return artifactLocationURI;
  }
}

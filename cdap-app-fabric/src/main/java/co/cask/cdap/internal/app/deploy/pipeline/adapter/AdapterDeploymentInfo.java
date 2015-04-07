/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline.adapter;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.internal.app.runtime.adapter.ApplicationTemplateInfo;
import co.cask.cdap.proto.AdapterConfig;
import org.apache.twill.filesystem.Location;

import java.io.File;

/**
 * Contains information needed for deployment of adapters.
 */
public class AdapterDeploymentInfo {
  private final AdapterConfig adapterConfig;
  private final ApplicationTemplateInfo templateInfo;
  private final ApplicationSpecification templateSpec;

  public AdapterDeploymentInfo(AdapterConfig adapterConfig, ApplicationTemplateInfo templateInfo,
                               ApplicationSpecification templateSpec) {
    this.adapterConfig = adapterConfig;
    this.templateInfo = templateInfo;
    this.templateSpec = templateSpec;
  }

  public AdapterConfig getAdapterConfig() {
    return adapterConfig;
  }

  public ApplicationTemplateInfo getTemplateInfo() {
    return templateInfo;
  }

  public ApplicationSpecification getTemplateSpec() {
    return templateSpec;
  }
}

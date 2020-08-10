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

import io.cdap.cdap.common.options.Option;

import javax.annotation.Nullable;

/**
 * Options for a MasterEnvironment.
 */
public class EnvironmentOptions {
  @Option(name = "env", usage = "Name of the CDAP master environment extension provider")
  private String envProvider;

  @Option(name = "conf", usage = "Directory path for CDAP configuration files")
  private String extraConfPath;

  @Option(name = "runnableClass", usage = "MasterEnvironmentRunnable class")
  private String runnableClass;

  @Nullable
  public String getEnvProvider() {
    return envProvider;
  }

  @Nullable
  public String getExtraConfPath() {
    return extraConfPath;
  }

  @Nullable
  public String getRunnableClass() {
    return runnableClass;
  }
}

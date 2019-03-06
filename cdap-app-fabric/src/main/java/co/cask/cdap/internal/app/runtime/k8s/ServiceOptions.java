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

package co.cask.cdap.internal.app.runtime.k8s;

import co.cask.cdap.common.options.Option;
import co.cask.cdap.master.environment.k8s.EnvironmentOptions;

/**
 * Environment options for application service programs.
 */
public class ServiceOptions extends EnvironmentOptions {
  public static final String APP_SPEC_PATH = "appSpecPath";
  public static final String PROGRAM_OPTIONS_PATH = "programOptions";
  public static final String BIND_ADDRESS = "bindAddress";

  @Option(name = APP_SPEC_PATH, usage = "Path to application specification file")
  private String appSpecPath;

  @Option(name = PROGRAM_OPTIONS_PATH, usage = "Path to program options file")
  private String programOptionsPath;

  @Option(name = BIND_ADDRESS, usage = "The address to bind to")
  private String bindAddress;

  public String getAppSpecPath() {
    return appSpecPath;
  }

  public String getProgramOptionsPath() {
    return programOptionsPath;
  }

  public String getBindAddress() {
    return bindAddress == null ? "0.0.0.0" : bindAddress;
  }
}

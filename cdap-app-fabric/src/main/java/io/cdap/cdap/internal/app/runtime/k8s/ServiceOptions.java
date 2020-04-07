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

package io.cdap.cdap.internal.app.runtime.k8s;

import io.cdap.cdap.common.options.Option;
import io.cdap.cdap.master.environment.k8s.EnvironmentOptions;

/**
 * Environment options for application service programs.
 */
public class ServiceOptions extends EnvironmentOptions {
  public static final String APP_SPEC_PATH = "appSpecPath";
  public static final String PROGRAM_OPTIONS_PATH = "programOptions";
  public static final String SYSTEM_TABLE_SPECS_PATH = "systemTableSpecs";
  public static final String BIND_ADDRESS = "bindAddress";
  public static final String TWILL_RUN_ID = "twillRunId";

  @Option(name = APP_SPEC_PATH, usage = "Path to application specification file")
  private String appSpecPath;

  @Option(name = PROGRAM_OPTIONS_PATH, usage = "Path to program options file")
  private String programOptionsPath;

  @Option(name = SYSTEM_TABLE_SPECS_PATH, usage = "Path to system table specification file")
  private String systemTableSpecPath;

  @Option(name = BIND_ADDRESS, usage = "The address to bind to")
  private String bindAddress;

  @Option(name = TWILL_RUN_ID, usage = "The twill run id")
  private String twillRunId;

  public String getAppSpecPath() {
    return appSpecPath;
  }

  public String getProgramOptionsPath() {
    return programOptionsPath;
  }

  public String getSystemTableSpecsPath() {
    return systemTableSpecPath;
  }

  public String getBindAddress() {
    return bindAddress == null ? "0.0.0.0" : bindAddress;
  }

  public String getTwillRunId() {
    return twillRunId;
  }
}

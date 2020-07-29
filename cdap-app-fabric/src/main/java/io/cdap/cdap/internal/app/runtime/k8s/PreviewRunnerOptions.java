/*
 * Copyright (C) 2020 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cdap.cdap.internal.app.runtime.k8s;

import io.cdap.cdap.common.options.Option;
import io.cdap.cdap.master.environment.k8s.EnvironmentOptions;

/**
 * Environment options for preview runners.
 */
public class PreviewRunnerOptions extends EnvironmentOptions {
  public static final String INSTANCE_NAME_FILE_PATH = "instanceNameFilePath";
  public static final String INSTANCE_UID_FILE_PATH = "instanceUidFilePath";

  @Option(name = INSTANCE_NAME_FILE_PATH, usage = "Path to file containing container name.")
  private String instanceNameFilePath;

  @Option(name = INSTANCE_UID_FILE_PATH, usage = "Path to file containing uid of container.")
  private String instanceUidFilePath;

  public String getInstanceNameFilePath() {
    return instanceNameFilePath;
  }

  public String getInstanceUidFilePath() {
    return instanceUidFilePath;
  }
}

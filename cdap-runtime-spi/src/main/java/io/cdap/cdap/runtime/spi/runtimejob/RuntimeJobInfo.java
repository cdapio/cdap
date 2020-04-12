/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.runtimejob;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import org.apache.twill.api.LocalFile;

import java.util.Collection;

/**
 * Runtime job information. The instance of this interface will be provided to
 * {@link RuntimeJobManager#launch(RuntimeJobInfo)} with files to be localized and runtime job class implementation.
 */
public interface RuntimeJobInfo {
  /**
   * Returns a collection of files that are used to launch the job.
   */
  Collection<? extends LocalFile> getLocalizeFiles();

  /**
   * Returns fully qualified classname of default implementation of a {@link RuntimeJob}.
   * This class is responsible for submitting runtime job to provided environment.
   */
  String getRuntimeJobClassname();

  /**
   * Returns a program run info.
   */
  ProgramRunInfo getProgramRunInfo();
}

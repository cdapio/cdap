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

import org.apache.twill.api.LocalFile;
import org.apache.twill.api.TwillRunnerService;

import java.util.Collection;

/**
 * Runtime job information. The instance of this interface will be provided to
 * {@link RuntimeJobManager#launch(RuntimeJobInfo)} with files to be localized and runtime job class implementation.
 */
public interface RuntimeJobInfo {
  /**
   * Returns a collection of program files that is used to launch the job.
   */
  public Collection<? extends LocalFile> getFilesToLocalize();

  /**
   * Returns default implementation of {@link RuntimeJob}. This class is responsible for executing program on
   * {@link TwillRunnerService}.
   */
  public Class<? extends RuntimeJob> getRuntimeJobClass();
}

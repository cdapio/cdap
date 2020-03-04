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

package io.cdap.cdap.runtime.spi.launcher;

import java.net.URI;
import java.util.List;

/**
 *
 */
public interface Launcher {

  /**
   *
   * @return
   */
  String getClassName();

  /**
   *
   * @param launchInfo
   * @return
   */
  JobId launch(LaunchInfo launchInfo);

  /**
   *
   * @return
   */
  URI getJarURI();

  /**
   * gets job details
   * @param jobId
   * @return
   * @throws Exception
   */
  default JobDetails getDetails(JobId jobId) throws Exception {
    return null;
  }

  /**
   * lists all in-transit or running jobs.
   * @return
   * @throws Exception
   */
  default List<JobDetails> list() throws Exception {
    return null;
  }

  /**
   * stops job
   *
   * @param jobId
   * @throws Exception
   */
  default void stop(JobId jobId) throws Exception {

  }
}

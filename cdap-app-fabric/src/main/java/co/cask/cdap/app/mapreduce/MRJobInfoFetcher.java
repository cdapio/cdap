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

package co.cask.cdap.app.mapreduce;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MRJobInfo;

/**
 * Retrieves information about a run of a MapReduce job.
 */
public interface MRJobInfoFetcher {

  /**
   * @param runId for which information will be returned.
   * @return a {@link MRJobInfo} containing information about a particular MapReduce program run.
   */
  MRJobInfo getMRJobInfo(Id.Run runId) throws Exception;

}

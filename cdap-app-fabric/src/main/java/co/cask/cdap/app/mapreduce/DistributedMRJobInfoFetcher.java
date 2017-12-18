/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves information about a run of a MapReduce job, using {@link MRJobClient} and
 * then {@link LocalMRJobInfoFetcher} if necessary.
 */
public class DistributedMRJobInfoFetcher implements MRJobInfoFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedMRJobInfoFetcher.class);
  private final MRJobClient mrJobClient;
  private final LocalMRJobInfoFetcher localMRJobInfoFetcher;

  @Inject
  public DistributedMRJobInfoFetcher(MRJobClient mrJobClient, LocalMRJobInfoFetcher localMRJobInfoFetcher) {
    this.mrJobClient = mrJobClient;
    this.localMRJobInfoFetcher = localMRJobInfoFetcher;
  }

  /**
   * Attempts to use MRJobClient to retrieve job information of a particular run.
   * If there is an IOException or NotFoundException, it will fall back to the Metrics System via MapReduceMetricsInfo.
   * @param runId for which information will be returned.
   * @return a {@link MRJobInfo} containing information about a particular MapReduce program run.
   */
  @Override
  public MRJobInfo getMRJobInfo(Id.Run runId) {
    try {
      return mrJobClient.getMRJobInfo(runId);
    } catch (Exception e) {
      LOG.debug("Failed to get run history from JobClient for runId: {}. Falling back to Metrics system.", runId, e);
      return localMRJobInfoFetcher.getMRJobInfo(runId);
    }
  }
}

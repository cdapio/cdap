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

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MRJobInfo;
import co.cask.cdap.proto.MRTaskInfo;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Wrapper around Hadoop JobClient that operates with CDAP Program RunIds.
 * This class is responsible for the MapReduce RunId->JobId mapping logic as well as to simplify the response
 * from the Job History Server.
 */
public class MRJobClient implements MRJobInfoFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(MRJobClient.class);
  private final Configuration hConf;

  @Inject
  public MRJobClient(CConfiguration cConf, Configuration hConf) {
    int numRetries = cConf.getInt(Constants.AppFabric.MAPREDUCE_JOB_CLIENT_CONNECT_MAX_RETRIES);
    this.hConf = new Configuration(hConf);
    // Override a cloned hConf's configuration of IPC Client max retries based upon value in CConf to avoid longer
    // amounts of retrying (this is helpful especially when the Job History Server is not installed)
    this.hConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, numRetries);
  }

  /**
   * @param runId for which information will be returned.
   * @return a {@link MRJobInfo} containing information about a particular MapReduce program run.
   * @throws IOException if there is failure to communicate through the JobClient.
   * @throws NotFoundException if a Job with the given runId is not found.
   */
  public MRJobInfo getMRJobInfo(Id.Run runId) throws IOException, NotFoundException {
    Preconditions.checkArgument(ProgramType.MAPREDUCE.equals(runId.getProgram().getType()));

    JobClient jobClient;
    JobStatus[] jobs;
    try {
      jobClient = new JobClient(hConf);
      jobs = jobClient.getAllJobs();
    } catch (Exception e) {
      LOG.warn("JobClient failed to get all jobs.", e);
      throw new IOException(e);
    }

    JobStatus thisJob = findJobForRunId(jobs, runId);

    RunningJob runningJob = jobClient.getJob(thisJob.getJobID());
    Counters counters = runningJob.getCounters();

    TaskReport[] mapTaskReports = jobClient.getMapTaskReports(thisJob.getJobID());
    TaskReport[] reduceTaskReports = jobClient.getReduceTaskReports(thisJob.getJobID());

    return new MRJobInfo(runningJob.mapProgress(), runningJob.reduceProgress(),
                         groupToMap(counters.getGroup(TaskCounter.class.getName())),
                         toMRTaskInfos(mapTaskReports), toMRTaskInfos(reduceTaskReports), true);
  }

  private JobStatus findJobForRunId(JobStatus[] jobs, Id.Run runId) throws NotFoundException {
    for (JobStatus job : jobs) {
      if (job.getJobName().startsWith(runId.getId())) {
        return job;
      }
    }
    throw new NotFoundException(runId);
  }

  // Converts a TaskReport to a simplified version of it - a MRTaskInfo.
  private List<MRTaskInfo> toMRTaskInfos(TaskReport[] taskReports) {
    List<MRTaskInfo> taskInfos = Lists.newArrayList();

    for (TaskReport taskReport : taskReports) {
      taskInfos.add(new MRTaskInfo(taskReport.getTaskId(), taskReport.getState(),
                                   taskReport.getStartTime(), taskReport.getFinishTime(), taskReport.getProgress(),
                                   groupToMap(taskReport.getCounters().getGroup(TaskCounter.class.getName()))));
    }
    return taskInfos;
  }

  // Given a Group object, returns a Map<CounterName, Value>
  private Map<String, Long> groupToMap(Counters.Group counterGroup) {
    Map<String, Long> counters = Maps.newHashMap();
    for (Counters.Counter counter : counterGroup) {
      counters.put(counter.getName(), counter.getValue());
    }
    return counters;
  }
}

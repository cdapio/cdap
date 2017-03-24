/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * ScheduleJob class is used in quartz scheduler job store. Retaining the DefaultSchedulerService$ScheduleJob
 * for backwards compatibility.
 * TODO: Refactor in 3.0.0
 */
public class DefaultSchedulerService {

  /**
   * Handler that gets called by quartz to execute a scheduled job.
   */
  public static final class ScheduledJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledJob.class);
    private final ScheduleTaskRunner taskRunner;

    ScheduledJob(Store store, ProgramLifecycleService lifecycleService, PropertiesResolver propertiesResolver,
                 ListeningExecutorService taskExecutor, NamespaceQueryAdmin namespaceQueryAdmin, CConfiguration cConf) {
      this.taskRunner = new ScheduleTaskRunner(store, lifecycleService, propertiesResolver, taskExecutor,
                                               namespaceQueryAdmin, cConf);
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      LOG.debug("Trying to run job {} with trigger {}", context.getJobDetail().getKey().toString(),
                context.getTrigger().getKey().toString());
      Trigger trigger = context.getTrigger();
      String key = trigger.getKey().getName();
      String[] parts = key.split(":");
      Preconditions.checkArgument(parts.length == 6, String.format("Trigger's key name %s has %d parts instead of 6",
                                  key, parts.length));

      String namespaceId = parts[0];
      String applicationId = parts[1];
      String appVersion = parts[2];
      ProgramType programType = ProgramType.valueOf(parts[3]);
      String programName = parts[4];
      String scheduleName = parts[5];

      LOG.debug("Schedule execute {}", key);
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      builder.put(ProgramOptionConstants.RETRY_COUNT, Integer.toString(context.getRefireCount()));
      builder.put(ProgramOptionConstants.SCHEDULE_NAME, scheduleName);

      Map<String, String> userOverrides = ImmutableMap.of(ProgramOptionConstants.LOGICAL_START_TIME,
                                                          Long.toString(context.getScheduledFireTime().getTime()));

      JobDataMap jobDataMap = trigger.getJobDataMap();
      for (Map.Entry<String, Object> entry : jobDataMap.entrySet()) {
        builder.put(entry.getKey(), jobDataMap.getString(entry.getKey()));
      }

      ProgramId programId = new ApplicationId(namespaceId, applicationId, appVersion).program(programType, programName);
      try {
        taskRunner.run(programId, builder.build(), userOverrides).get();
      } catch (TaskExecutionException e) {
        LOG.warn("Error while running program {}. {}", programId, e);
        throw new JobExecutionException(e.getMessage(), e.getCause(), e.isRefireImmediately());
      } catch (Throwable t) {
        // Do not remove this log line. The exception at higher level gets caught by the quartz scheduler and is not
        // logged in cdap master logs making it hard to debug issues.
        LOG.warn("Error while running program {}. {}", programId, t);
        throw new JobExecutionException(t.getMessage(), t.getCause(), false);
      }
    }
  }
}

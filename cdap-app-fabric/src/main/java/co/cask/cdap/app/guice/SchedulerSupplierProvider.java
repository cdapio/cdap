/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.app.guice;


import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.schedule.ExecutorThreadPool;
import co.cask.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.StdJobRunShellFactory;
import org.quartz.impl.StdScheduler;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;

/**
 *
 */
public class SchedulerSupplierProvider implements Provider<Supplier<Scheduler>> {
  final DatasetBasedTimeScheduleStore scheduleStore;
  final CConfiguration cConf;

  @Inject
  SchedulerSupplierProvider(DatasetBasedTimeScheduleStore scheduleStore, CConfiguration cConf) {
    this.scheduleStore = scheduleStore;
    this.cConf = cConf;
  }

  /**
   * Provides a supplier of quartz scheduler so that initialization of the scheduler can be done after guice
   * injection. It returns a singleton of Scheduler.
   */
  @Override
  public Supplier<Scheduler> get() {
    return new Supplier<org.quartz.Scheduler>() {
      private org.quartz.Scheduler scheduler;

      @Override
      public synchronized org.quartz.Scheduler get() {
        try {
          if (scheduler == null) {
            scheduler = getScheduler(scheduleStore, cConf);
          }
          return scheduler;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /**
   * Create a quartz scheduler. Quartz factory method is not used, because inflexible in allowing custom jobstore
   * and turning off check for new versions.
   * @param store JobStore.
   * @param cConf CConfiguration.
   * @return an instance of {@link org.quartz.Scheduler}
   * @throws SchedulerException
   */
  private org.quartz.Scheduler getScheduler(JobStore store,
                                            CConfiguration cConf) throws SchedulerException {

    int threadPoolSize = cConf.getInt(Constants.Scheduler.CFG_SCHEDULER_MAX_THREAD_POOL_SIZE);
    ExecutorThreadPool threadPool = new ExecutorThreadPool(threadPoolSize);
    threadPool.initialize();
    String schedulerName = DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME;
    String schedulerInstanceId = DirectSchedulerFactory.DEFAULT_INSTANCE_ID;

    QuartzSchedulerResources qrs = new QuartzSchedulerResources();
    JobRunShellFactory jrsf = new StdJobRunShellFactory();

    qrs.setName(schedulerName);
    qrs.setInstanceId(schedulerInstanceId);
    qrs.setJobRunShellFactory(jrsf);
    qrs.setThreadPool(threadPool);
    qrs.setThreadExecutor(new DefaultThreadExecutor());
    qrs.setJobStore(store);
    qrs.setRunUpdateCheck(false);
    QuartzScheduler qs = new QuartzScheduler(qrs, -1, -1);

    ClassLoadHelper cch = new CascadingClassLoadHelper();
    cch.initialize();

    store.initialize(cch, qs.getSchedulerSignaler());
    org.quartz.Scheduler scheduler = new StdScheduler(qs);

    jrsf.initialize(scheduler);
    qs.initialize();

    return scheduler;
  }
}

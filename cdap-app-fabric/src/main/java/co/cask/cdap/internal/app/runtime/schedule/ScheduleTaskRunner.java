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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.queue.Job;
import co.cask.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerInfoContext;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;

/**
 * Task runner that runs a schedule.
 */
public final class ScheduleTaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleTaskRunner.class);
  private static final Gson GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder())
    .create();

  private final Store store;
  private final ProgramLifecycleService lifecycleService;
  private final PropertiesResolver propertiesResolver;
  private final ListeningExecutorService executorService;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;

  public ScheduleTaskRunner(Store store, ProgramLifecycleService lifecycleService,
                            PropertiesResolver propertiesResolver, ListeningExecutorService taskExecutor,
                            NamespaceQueryAdmin namespaceQueryAdmin, CConfiguration cConf) {
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.executorService = taskExecutor;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
  }

  public void launch(Job job) throws Exception {
    ProgramSchedule schedule = job.getSchedule();
    ProgramId programId = schedule.getProgramId();

    Map<String, String> userArgs = Maps.newHashMap();
    userArgs.putAll(schedule.getProperties());
    userArgs.putAll(propertiesResolver.getUserProperties(programId.toId()));

    Map<String, String> systemArgs = Maps.newHashMap();
    systemArgs.putAll(propertiesResolver.getSystemProperties(programId.toId()));

    // Let the triggers update the arguments first before setting the triggering schedule info
    ((SatisfiableTrigger) job.getSchedule().getTrigger()).updateLaunchArguments(job.getSchedule(),
                                                                                job.getNotifications(),
                                                                                userArgs, systemArgs);

    TriggeringScheduleInfo triggeringScheduleInfo = getTriggeringScheduleInfo(job);
    systemArgs.put(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, GSON.toJson(triggeringScheduleInfo));

    execute(programId, systemArgs, userArgs);
    LOG.info("Successfully started program {} in schedule {}.", schedule.getProgramId(), schedule.getName());
  }

  private TriggeringScheduleInfo getTriggeringScheduleInfo(Job job) {
    TriggerInfoContext triggerInfoContext = new TriggerInfoContext(job, store);
    SatisfiableTrigger trigger = ((SatisfiableTrigger) job.getSchedule().getTrigger());
    List<TriggerInfo> triggerInfo = trigger.getTriggerInfos(triggerInfoContext);

    ProgramSchedule schedule = job.getSchedule();
    return new TriggeringScheduleInfo(schedule.getName(), schedule.getDescription(),
                                      triggerInfo, schedule.getProperties());
  }

  /**
   * Executes a program without blocking until its completion.
   *
   * @return a {@link ListenableFuture} object that completes when the program completes
   */
  public ListenableFuture<?> execute(final ProgramId id, Map<String, String> sysArgs,
                                     Map<String, String> userArgs) throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo;
    String originalUserId = SecurityRequestContext.getUserId();
    try {
      // if the program has a namespace user configured then set that user in the security request context.
      // See: CDAP-7396
      String nsPrincipal = namespaceQueryAdmin.get(id.getNamespaceId()).getConfig().getPrincipal();
      if (nsPrincipal != null && SecurityUtil.isKerberosEnabled(cConf)) {
        SecurityRequestContext.setUserId(new KerberosName(nsPrincipal).getServiceName());
      }
      runtimeInfo = lifecycleService.startInternal(id, sysArgs, userArgs, false);
    } catch (ProgramNotFoundException | ApplicationNotFoundException e) {
      throw new TaskExecutionException(String.format(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND), id),
                                       e, false);
    } finally {
      SecurityRequestContext.setUserId(originalUserId);
    }

    final ProgramController controller = runtimeInfo.getController();
    final CountDownLatch latch = new CountDownLatch(1);

    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State state, @Nullable Throwable cause) {
        if (state == ProgramController.State.COMPLETED) {
          completed();
        }
        if (state == ProgramController.State.ERROR) {
          error(controller.getFailureCause());
        }
      }

      @Override
      public void killed() {
        latch.countDown();
      }

      @Override
      public void completed() {
        latch.countDown();
      }

      @Override
      public void error(Throwable cause) {
        latch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        latch.await();
        return null;
      }
    });
  }
}

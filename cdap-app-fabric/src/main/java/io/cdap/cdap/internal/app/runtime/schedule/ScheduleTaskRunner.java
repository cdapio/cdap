/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.UserErrors;
import io.cdap.cdap.internal.UserMessages;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerInfoContext;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.PropertiesResolver;
import io.cdap.cdap.internal.capability.CapabilityNotAvailableException;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;

  public ScheduleTaskRunner(Store store, ProgramLifecycleService lifecycleService,
                            PropertiesResolver propertiesResolver,
                            NamespaceQueryAdmin namespaceQueryAdmin, CConfiguration cConf) {
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
  }

  public void launch(Job job) throws Exception {
    ProgramSchedule schedule = job.getSchedule();
    ProgramId programId = schedule.getProgramId();

    Map<String, String> userArgs = getUserArgs(schedule, propertiesResolver);

    Map<String, String> systemArgs = new HashMap<>(propertiesResolver.getSystemProperties(programId));

    // Let the triggers update the arguments first before setting the triggering schedule info
    ((SatisfiableTrigger) job.getSchedule().getTrigger()).updateLaunchArguments(job.getSchedule(),
                                                                                job.getNotifications(),
                                                                                userArgs, systemArgs);

    TriggeringScheduleInfo triggeringScheduleInfo = getTriggeringScheduleInfo(job);
    systemArgs.put(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, GSON.toJson(triggeringScheduleInfo));

    try {
      execute(programId, systemArgs, userArgs);
      LOG.info("Successfully started program {} in schedule {}.", schedule.getProgramId(), schedule.getName());
    } catch (CapabilityNotAvailableException ex) {
      LOG.debug("Ignoring program {} in schedule {}.", schedule.getProgramId(), schedule.getName(), ex);
    }
  }

  @VisibleForTesting
  static Map<String, String> getUserArgs(ProgramSchedule schedule,
                                         PropertiesResolver propertiesResolver) throws IOException, NotFoundException {
    Map<String, String> userArgs = new HashMap<>();
    userArgs.putAll(propertiesResolver.getUserProperties(schedule.getProgramId()));
    userArgs.putAll(schedule.getProperties());
    return userArgs;
  }

  private TriggeringScheduleInfo getTriggeringScheduleInfo(Job job) {
    TriggerInfoContext triggerInfoContext = new TriggerInfoContext(job, store);
    SatisfiableTrigger trigger = ((SatisfiableTrigger) job.getSchedule().getTrigger());
    List<TriggerInfo> triggerInfo = trigger.getTriggerInfos(triggerInfoContext);

    ProgramSchedule schedule = job.getSchedule();
    return new DefaultTriggeringScheduleInfo(schedule.getName(), schedule.getDescription(),
                                             triggerInfo, schedule.getProperties());
  }

  /**
   * Executes a program without blocking until its completion.
   */
  public void execute(ProgramId id, Map<String, String> sysArgs, Map<String, String> userArgs) throws Exception {
    String originalUserId = SecurityRequestContext.getUserId();
    try {
      // if the program has a namespace user configured then set that user in the security request context.
      // See: CDAP-7396
      String nsPrincipal = namespaceQueryAdmin.get(id.getNamespaceId()).getConfig().getPrincipal();
      if (nsPrincipal != null && SecurityUtil.isKerberosEnabled(cConf)) {
        SecurityRequestContext.setUserId(new KerberosName(nsPrincipal).getServiceName());
      }
      lifecycleService.runInternal(id, userArgs, sysArgs, false);
    } catch (ProgramNotFoundException | ApplicationNotFoundException e) {
      throw new TaskExecutionException(String.format(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND), id),
                                       e, false);
    } finally {
      SecurityRequestContext.setUserId(originalUserId);
    }
  }
}

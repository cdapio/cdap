/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.RunIds;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link TwillControllerCreator} for On-Premise(Native) Cluster mode.
 */
public class NativeTwillControllerCreator implements TwillControllerCreator {

  private final TwillRunnerService twillRunnerService;
  private final CConfiguration cConf;
  private final Set<String> namespaces;

  @Inject
  public NativeTwillControllerCreator(CConfiguration cConf, TwillRunnerService twillRunnerService) {
    this.cConf = cConf;
    this.twillRunnerService = twillRunnerService;
    namespaces = new HashSet<>();
  }

  @Override
  public TwillController createTwillController(ProgramRunId programRunId) throws Exception {
    AtomicReference<TwillController> twillController = new AtomicReference<>();
    RetryStrategy retryStrategy =
      RetryStrategies.timeLimit(cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS), TimeUnit.SECONDS,
                                RetryStrategies.exponentialDelay(10, 1000, TimeUnit.MILLISECONDS));

    long startRetry = System.currentTimeMillis();
    // TODO throw proper exception if retries fail
    Retries.runWithRetries(() -> {
      /**
       * Under two scenarios, twillController might be null:
       * (1) TwillController has not been added to the twillRunnerService, and it will be added later.
       * (2) TwillController has been removed from twillRunnerService.
       */
      twillController.set(twillRunnerService.lookup(TwillAppNames.toTwillAppName(
                                                      programRunId.getParent()),
                                                    RunIds.fromString(Objects.requireNonNull(programRunId.getRun()))));
    }, retryStrategy, e -> twillController.get() == null);

    long retryDuration = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startRetry);
    CountDownLatch latch = new CountDownLatch(1);
    twillController.get().onRunning(latch::countDown, Threads.SAME_THREAD_EXECUTOR);
    twillController.get().onTerminated(latch::countDown, Threads.SAME_THREAD_EXECUTOR);
    latch.await(cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS) - retryDuration, TimeUnit.SECONDS);
    return twillController.get();

  }
}

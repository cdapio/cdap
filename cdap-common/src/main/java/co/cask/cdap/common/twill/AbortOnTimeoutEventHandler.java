/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.common.twill;

import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.EventHandlerContext;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A Twill {@link EventHandler} that abort the application if for some runnable it cannot provision container for
 * too long.
 */
public class AbortOnTimeoutEventHandler extends EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbortOnTimeoutEventHandler.class);

  private long abortTime;
  private boolean abortIfNotFull;
  private String applicationName;
  private RunId runId;

  /**
   * Constructs an instance of AbortOnTimeoutEventHandler that abort the application if some runnable has no
   * containers, same as calling {@link #AbortOnTimeoutEventHandler(long, boolean)} with second parameter as
   * {@code false}.
   *
   * @param abortTime Time in milliseconds to pass before aborting the application if no container is given to
   *                  a runnable.
   */
  public AbortOnTimeoutEventHandler(long abortTime) {
    this(abortTime, false);
  }

  /**
   * Constructs an instance of AbortOnTimeoutEventHandler that abort the application if some runnable has not enough
   * containers.
   * @param abortTime Time in milliseconds to pass before aborting the application if no container is given to
   *                  a runnable.
   * @param abortIfNotFull If {@code true}, it will abort the application if any runnable doesn't meet the expected
   *                       number of instances.
   */
  public AbortOnTimeoutEventHandler(long abortTime, boolean abortIfNotFull) {
    this.abortTime = abortTime;
    this.abortIfNotFull = abortIfNotFull;
  }

  @Override
  protected Map<String, String> getConfigs() {
    return ImmutableMap.of("abortTime", Long.toString(abortTime),
                           "abortIfNotFull", Boolean.toString(abortIfNotFull));
  }

  @Override
  public void initialize(EventHandlerContext context) {
    super.initialize(context);
    this.abortTime = Long.parseLong(context.getSpecification().getConfigs().get("abortTime"));
    this.abortIfNotFull = Boolean.parseBoolean(context.getSpecification().getConfigs().get("abortIfNotFull"));
    this.applicationName = context.getApplicationName();
    this.runId = context.getRunId();
  }


  @Override
  public TimeoutAction launchTimeout(Iterable<TimeoutEvent> timeoutEvents) {
    long now = System.currentTimeMillis();
    for (TimeoutEvent event : timeoutEvents) {
      LOG.warn("Requested {} containers for runnable {} when running application {} with run id {}," +
                 " only got {} after {} ms.",
               event.getExpectedInstances(), event.getRunnableName(), applicationName, runId,
               event.getActualInstances(), System.currentTimeMillis() - event.getRequestTime());

      boolean pass = abortIfNotFull ? event.getActualInstances() == event.getExpectedInstances()
                                    : event.getActualInstances() != 0;
      if (!pass && (now - event.getRequestTime()) > abortTime) {
        LOG.error("No containers for runnable {} when running application {} with run id {}. Abort the application.",
                  event.getRunnableName(), applicationName, runId);
        return TimeoutAction.abort();
      }
    }
    // Check again in half of abort time.
    return TimeoutAction.recheck(abortTime / 2, TimeUnit.MILLISECONDS);
  }
}

/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.service;

import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver for InMemory service runnable
 */
public class InMemoryRunnableDriver extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryRunnableDriver.class);
  private TwillRunnable runnable;
  private TwillContext context;
  private LoggingContext loggingContext;

  public InMemoryRunnableDriver(TwillRunnable runnable, TwillContext context,
                                LoggingContext loggingContext) {
    this.runnable = runnable;
    this.context = context;
    this.loggingContext = loggingContext;
  }

  @Override
  protected void startUp() {
    LoggingContextAccessor.setLoggingContext(loggingContext);
    runnable.initialize(context);
  }

  @Override
  protected void triggerShutdown() {
    runnable.stop();
    LOG.info("Runnable {} Stopped ", context.getSpecification().getName());
  }

  private void destroy() throws Exception {
    runnable.destroy();
    LOG.info("Runnable {} Destroyed ", context.getSpecification().getName());
  }

  @Override
  protected void run() throws Exception {
    try {
      runnable.run();
    } catch (Exception ex) {
      LOG.info(" Starting Runnable {} Failed ", context.getSpecification().getName());
    } finally {
      destroy();
    }
  }
}

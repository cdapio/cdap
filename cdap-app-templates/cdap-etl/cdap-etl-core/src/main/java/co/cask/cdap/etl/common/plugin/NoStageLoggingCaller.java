/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.common.plugin;

import co.cask.cdap.etl.common.Constants;
import org.slf4j.MDC;

import java.util.concurrent.Callable;

/**
 * Clears the stage name from the Logging MDC before calling a callable and resets it when finished. This is useful
 * when a plugin is calling a CDAP method, as we don't want the log messages from CDAP to have the stage name
 * in its log messages.
 */
public class NoStageLoggingCaller extends Caller {
  private final Caller delegate;

  private NoStageLoggingCaller(Caller delegate) {
    this.delegate = delegate;
  }

  @Override
  public <T> T call(Callable<T> callable, CallArgs args) throws Exception {
    String stage = MDC.get(Constants.MDC_STAGE_KEY);
    if (stage == null) {
      return delegate.call(callable, args);
    }

    MDC.remove(Constants.MDC_STAGE_KEY);
    try {
      return delegate.call(callable, args);
    } finally {
      MDC.put(Constants.MDC_STAGE_KEY, stage);
    }
  }

  public static Caller wrap(Caller delegate) {
    return new NoStageLoggingCaller(delegate);
  }
}

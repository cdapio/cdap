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
 * Sets the stage name in logging MDC before calling the callable, and resets it when finished.
 */
public class StageLoggingCaller extends Caller {
  private final Caller delegate;
  private final String stageName;

  private StageLoggingCaller(Caller delegate, String stageName) {
    this.delegate = delegate;
    this.stageName = stageName;
  }

  @Override
  public <T> T call(Callable<T> callable, CallArgs args) throws Exception {
    MDC.put(Constants.MDC_STAGE_KEY, stageName);
    try {
      return delegate.call(callable, args);
    } finally {
      MDC.remove(Constants.MDC_STAGE_KEY);
    }
  }

  public static Caller wrap(Caller delegate, String stageName) {
    return new StageLoggingCaller(delegate, stageName);
  }
}

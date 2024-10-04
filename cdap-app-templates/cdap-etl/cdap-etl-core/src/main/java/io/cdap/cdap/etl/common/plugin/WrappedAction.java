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

package io.cdap.cdap.etl.common.plugin;

import io.cdap.cdap.api.exception.WrappedException;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Wrapper around {@link Action} that makes sure logging, classloading, and other pipeline
 * capabilities are setup correctly.
 */
public class WrappedAction extends Action implements PluginWrapper<Action> {

  private final Action action;
  private final Caller caller;
  private static final Logger LOG = LoggerFactory.getLogger(WrappedAction.class);

  public WrappedAction(Action action, Caller caller) {
    this.action = action;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked((Callable<Void>) () -> {
      action.configurePipeline(pipelineConfigurer);
      return null;
    });
  }

  @Override
  public void run(final ActionContext context) throws Exception {
    try {
      caller.call((Callable<Void>) () -> {
        action.run(context);
        return null;
      });
    } catch(Exception e) {
      if (caller instanceof StageLoggingCaller) {
        String stageName = ((StageLoggingCaller) caller).getStageName();
        MDC.put("Failed_Stage", stageName);
        LOG.error("Stage: {}", stageName);
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }

  @Override
  public Action getWrapped() {
    return action;
  }
}

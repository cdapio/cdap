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

import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;

import java.util.concurrent.Callable;

/**
 * Wrapper around {@link Action} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 */
public class WrappedAction extends Action {
  private final Action action;
  private final Caller caller;

  public WrappedAction(Action action, Caller caller) {
    this.action = action;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        action.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public void run(final ActionContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        action.run(context);
        return null;
      }
    });
  }
}

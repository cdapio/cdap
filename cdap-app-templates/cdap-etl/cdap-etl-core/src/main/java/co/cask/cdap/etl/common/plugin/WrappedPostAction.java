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
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;

import java.util.concurrent.Callable;

/**
 * Wrapper around {@link PostAction} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 */
public class WrappedPostAction extends PostAction {
  private final PostAction postAction;
  private final Caller caller;

  public WrappedPostAction(PostAction postAction, Caller caller) {
    this.postAction = postAction;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked((Callable<Void>) () -> {
      postAction.configurePipeline(pipelineConfigurer);
      return null;
    });
  }

  @Override
  public void propagateSchema(StageConfigurer stageConfigurer) {
    caller.callUnchecked(() -> {
      postAction.propagateSchema(stageConfigurer);
      return null;
    });
  }

  @Override
  public void run(BatchActionContext context) throws Exception {
    caller.call((Callable<Void>) () -> {
      postAction.run(context);
      return null;
    });
  }
}

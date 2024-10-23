/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.cdap.etl.api.MultiInputPipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Wrapper around {@link BatchAutoJoiner} that makes sure logging, classloading, and other pipeline
 * capabilities are set up correctly.
 *
 */
public class WrappedBatchAutoJoiner extends BatchAutoJoiner
  implements PluginWrapper<BatchAutoJoiner> {

  private final BatchAutoJoiner joiner;
  private final Caller caller;

  public WrappedBatchAutoJoiner(BatchAutoJoiner joiner, Caller caller) {
    this.joiner = joiner;
    this.caller = caller;
  }


  @Override
  public void configurePipeline(MultiInputPipelineConfigurer multiInputPipelineConfigurer) {
    caller.callUnchecked((Callable<Void>) () -> {
      joiner.configurePipeline(multiInputPipelineConfigurer);
      return null;
    });
  }

  @Override
  public void prepareRun(BatchJoinerContext context) throws Exception {
    caller.call((Callable<Void>) () -> {
      joiner.prepareRun(context);
      return null;
    });
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchJoinerContext context) {
    caller.callUnchecked((Callable<Void>) () -> {
      joiner.onRunFinish(succeeded, context);
      return null;
    });
  }

  @Nullable
  @Override
  public JoinDefinition define(AutoJoinerContext context) {
    return caller.callUnchecked(() -> joiner.define(context));
  }

  @Override
  public BatchAutoJoiner getWrapped() {
    return joiner;
  }
}

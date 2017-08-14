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

import co.cask.cdap.etl.api.JoinConfig;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.MultiInputPipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerContext;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.common.TypeChecker;

import java.util.concurrent.Callable;

/**
 * Wrapper around {@link BatchJoiner} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <JOIN_KEY> type of join key. Must be a supported type
 * @param <INPUT_RECORD> type of input record. Must be a supported type
 * @param <OUT> type of output object
 */
public class WrappedBatchJoiner<JOIN_KEY, INPUT_RECORD, OUT> extends BatchJoiner<JOIN_KEY, INPUT_RECORD, OUT> {
  private final BatchJoiner<JOIN_KEY, INPUT_RECORD, OUT> joiner;
  private final Caller caller;
  private final OperationTimer operationTimer;

  public WrappedBatchJoiner(BatchJoiner<JOIN_KEY, INPUT_RECORD, OUT> joiner, Caller caller,
                            OperationTimer operationTimer) {
    this.joiner = joiner;
    this.caller = caller;
    this.operationTimer = operationTimer;
  }

  @Override
  public void configurePipeline(final MultiInputPipelineConfigurer multiInputPipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        joiner.configurePipeline(multiInputPipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public void prepareRun(final BatchJoinerContext context) throws Exception {
    context.setJoinKeyClass(TypeChecker.getJoinKeyClass(joiner));
    context.setJoinInputRecordClass(TypeChecker.getJoinInputRecordClass(joiner));
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        joiner.prepareRun(context);
        return null;
      }
    });
  }

  @Override
  public void initialize(final BatchJoinerRuntimeContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        joiner.initialize(context);
        return null;
      }
    });
  }

  @Override
  public void destroy() {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        joiner.destroy();
        return null;
      }
    });
  }

  @Override
  public void onRunFinish(final boolean succeeded, final BatchJoinerContext context) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        joiner.onRunFinish(succeeded, context);
        return null;
      }
    });
  }

  @Override
  public JOIN_KEY joinOn(final String stageName, final INPUT_RECORD inputRecord) throws Exception {
    operationTimer.start();
    try {
      return caller.call(new Callable<JOIN_KEY>() {
        @Override
        public JOIN_KEY call() throws Exception {
          return joiner.joinOn(stageName, inputRecord);
        }
      });
    } finally {
      operationTimer.reset();
    }
  }

  @Override
  public JoinConfig getJoinConfig() throws Exception {
    return caller.call(new Callable<JoinConfig>() {
      @Override
      public JoinConfig call() throws Exception {
        return joiner.getJoinConfig();
      }
    });
  }

  @Override
  public OUT merge(final JOIN_KEY joinKey, final Iterable<JoinElement<INPUT_RECORD>> joinResult) throws Exception {
    operationTimer.start();
    try {
      return caller.call(new Callable<OUT>() {
        @Override
        public OUT call() throws Exception {
          return joiner.merge(joinKey, joinResult);
        }
      });
    } finally {
      operationTimer.reset();
    }
  }
}

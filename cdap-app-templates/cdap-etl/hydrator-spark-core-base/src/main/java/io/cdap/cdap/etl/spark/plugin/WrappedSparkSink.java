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

package io.cdap.cdap.etl.spark.plugin;

import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.common.plugin.Caller;
import org.apache.spark.api.java.JavaRDD;

import java.util.concurrent.Callable;

/**
 * Wrapper around a {@link SparkCompute} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <IN> The type of input record to the SparkSink.
 */
public class WrappedSparkSink<IN> extends SparkSink<IN> {
  private final SparkSink<IN> sink;
  private final Caller caller;

  public WrappedSparkSink(SparkSink<IN> sink, Caller caller) {
    this.sink = sink;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        sink.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        sink.prepareRun(context);
        return null;
      }
    });
  }

  @Override
  public void onRunFinish(boolean succeeded, SparkPluginContext context) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        sink.onRunFinish(succeeded, context);
        return null;
      }
    });
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<IN> input) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        sink.run(context, input);
        return null;
      }
    });
  }
}

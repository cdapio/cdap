/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSource;
import io.cdap.cdap.etl.common.plugin.Caller;
import org.apache.spark.api.java.JavaRDD;

import java.util.concurrent.Callable;

public class WrappedSparkSource<OUT> extends SparkSource<OUT> {
  private final SparkSource<OUT> source;
  private final Caller caller;

  public WrappedSparkSource(SparkSource<OUT> source, Caller caller) {
    this.source = source;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        source.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }


  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        source.prepareRun(context);
        return null;
      }
    });
  }

  @Override
  public void onRunFinish(boolean succeeded, SparkPluginContext context) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        source.onRunFinish(succeeded, context);
        return null;
      }
    });
  }

  @Override
  public JavaRDD<OUT> create(SparkExecutionPluginContext context) throws Exception {
    return caller.call(new Callable<JavaRDD<OUT>>() {
      @Override
      public JavaRDD<OUT> call() throws Exception {
        return source.create(context);
      }
    });
  }
}

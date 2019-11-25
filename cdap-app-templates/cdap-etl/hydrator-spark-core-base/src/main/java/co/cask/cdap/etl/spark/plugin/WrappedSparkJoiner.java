/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.plugin;

import co.cask.cdap.etl.api.MultiInputPipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.BatchJoinerContext;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkJoiner;
import co.cask.cdap.etl.common.plugin.Caller;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;
import java.util.concurrent.Callable;

public class WrappedSparkJoiner<OUT> extends SparkJoiner<OUT> {

    private final SparkJoiner joiner;
    private final Caller caller;

    public WrappedSparkJoiner(SparkJoiner joiner, Caller caller) {
        this.joiner = joiner;
        this.caller = caller;
    }

    @Override
    public void configurePipeline(MultiInputPipelineConfigurer multiInputPipelineConfigurer) throws IllegalArgumentException {
        caller.callUnchecked(new Callable<Object>() {
            @Override
            public Object call() throws IllegalArgumentException {
                joiner.configurePipeline(multiInputPipelineConfigurer);
                return null;
            }
        });
    }

    @Override
    public void initialize(SparkExecutionPluginContext context) throws Exception {
        caller.call(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                joiner.initialize(context);
                return null;
            }
        });
    }

    @Override
    public JavaRDD<OUT> join(SparkExecutionPluginContext context, Map<String, JavaRDD<?>> inputs) throws Exception {
        return caller.call(new Callable<JavaRDD<OUT>>() {
            @Override
            public JavaRDD<OUT> call() throws Exception {
                return joiner.join(context, inputs);
            }
        });
    }

    @Override
    public void prepareRun(BatchJoinerContext context) throws Exception {
        caller.call(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                joiner.prepareRun(context);
                return null;
            }
        });
    }

    @Override
    public void onRunFinish(boolean succeeded, BatchJoinerContext context) {
        caller.callUnchecked(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                joiner.onRunFinish(succeeded, context);
                return null;
            }
        });

    }
}

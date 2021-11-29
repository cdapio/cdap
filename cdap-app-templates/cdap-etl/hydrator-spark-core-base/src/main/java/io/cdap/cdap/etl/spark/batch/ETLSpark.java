/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.FieldOperationTypeAdapter;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.SetMultimapCodec;
import io.cdap.cdap.etl.common.submit.CompositeFinisher;
import io.cdap.cdap.etl.common.submit.Finisher;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Configures and sets up runs of {@link BatchSparkPipelineDriver}.
 */
public class ETLSpark extends AbstractSpark {
  private static final Logger LOG = LoggerFactory.getLogger(ETLSpark.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(DatasetInfo.class, new DatasetInfoTypeAdapter())
    .registerTypeAdapter(InputFormatProvider.class, new InputFormatProviderTypeAdapter())
    .registerTypeAdapter(FieldOperation.class, new FieldOperationTypeAdapter())
    .create();

  private final BatchPhaseSpec phaseSpec;
  private final RuntimeConfigurer runtimeConfigurer;
  private final String deployedNamespace;
  private Finisher finisher;

  public ETLSpark(BatchPhaseSpec phaseSpec, @Nullable RuntimeConfigurer runtimeConfigurer, String deployedNamespace) {
    this.phaseSpec = phaseSpec;
    this.runtimeConfigurer = runtimeConfigurer;
    this.deployedNamespace = deployedNamespace;
  }

  @Override
  protected void configure() {
    setName(phaseSpec.getPhaseName());
    setDescription(phaseSpec.getDescription());

    // register the plugins at program level so that the program can be failed by the platform early in case of
    // plugin requirements not being meet
    phaseSpec.getPhase().registerPlugins(getConfigurer(), runtimeConfigurer, deployedNamespace);

    setMainClass(BatchSparkPipelineDriver.class);

    setExecutorResources(phaseSpec.getResources());
    setDriverResources(phaseSpec.getDriverResources());
    setClientResources(phaseSpec.getClientResources());

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec, BatchPhaseSpec.class));
    setProperties(properties);
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void initialize() throws Exception {
    SparkClientContext context = getContext();

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.speculation", "false");
    // turn off auto-broadcast by default until we better understand the implications and can set this to a
    // value that we are confident is safe.
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1");
    sparkConf.set("spark.maxRemoteBlockSizeFetchToMem", String.valueOf(Integer.MAX_VALUE - 512));
    sparkConf.set("spark.network.timeout", "600s");
    // Disable yarn app retries since spark already performs retries at a task level.
    sparkConf.set("spark.yarn.maxAppAttempts", "1");
    // to make sure fields that are the same but different casing are treated as different fields in auto-joins
    // see CDAP-17024
    sparkConf.set("spark.sql.caseSensitive", "true");
    context.setSparkConf(sparkConf);

    Map<String, String> properties = context.getSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);

    for (Map.Entry<String, String> pipelineProperty : phaseSpec.getPipelineProperties().entrySet()) {
      sparkConf.set(pipelineProperty.getKey(), pipelineProperty.getValue());
    }

    PipelineRuntime pipelineRuntime = new PipelineRuntime(context);
    MacroEvaluator evaluator = new DefaultMacroEvaluator(pipelineRuntime.getArguments(),
                                                         context.getLogicalStartTime(), context,
                                                         context, context.getNamespace());
    SparkPreparer preparer = new SparkPreparer(context, context.getMetrics(), evaluator, pipelineRuntime);
    List<Finisher> finishers = preparer.prepare(phaseSpec);
    finisher = new CompositeFinisher(finishers);
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void destroy() {
    if (finisher != null) {
      finisher.onFinish(getContext().getState().getStatus() == ProgramStatus.COMPLETED);
    }
  }
}

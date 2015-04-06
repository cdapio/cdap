/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultTransformContext;
import co.cask.cdap.templates.etl.common.TransformExecutor;
import co.cask.cdap.templates.etl.common.config.ETLStage;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * MapReduce driver for Batch ETL Adapters.
 */
public class ETLMapReduce extends AbstractMapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final Gson GSON = new Gson();

  @Override
  public void configure() {
    setName("ETLMapReduce");
    setDescription("MapReduce driver for Batch ETL Adapters");
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    Map<String, String> runtimeArgs = context.getRuntimeArguments();

    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.ADAPTER_NAME));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.CONFIG_KEY));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Source.SPECIFICATION));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Sink.SPECIFICATION));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Transform.SPECIFICATIONS));

    ETLBatchConfig config = GSON.fromJson(runtimeArgs.get(Constants.CONFIG_KEY), ETLBatchConfig.class);
    StageSpecification sourceSpec = GSON.fromJson(runtimeArgs.get(Constants.Source.SPECIFICATION),
                                                  StageSpecification.class);
    StageSpecification sinkSpec = GSON.fromJson(runtimeArgs.get(Constants.Sink.SPECIFICATION),
                                                StageSpecification.class);

    prepareSource(sourceSpec, context, config.getSource());
    prepareSink(sinkSpec, context, config.getSink());
    prepareTransforms(context, config.getTransforms());

    job.setMapperClass(ETLMapper.class);
    job.setNumReduceTasks(0);
  }

  private void prepareSource(StageSpecification sourceSpec, MapReduceContext context, ETLStage sourceStage)
    throws Exception {
    BatchSource source = (BatchSource) Class.forName(sourceSpec.getClassName()).newInstance();
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, sourceStage, sourceSpec);
    LOG.info("Source Stage : {}", sourceStage);
    LOG.info("Source Class : {}", source.getClass().getName());
    LOG.info("Specifications of Source : {}", sourceSpec);
    source.prepareJob(sourceContext);
  }

  private void prepareSink(StageSpecification sinkSpec, MapReduceContext context, ETLStage sinkStage) throws Exception {
    BatchSink sink = (BatchSink) Class.forName(sinkSpec.getClassName()).newInstance();
    BatchSinkContext sinkContext = new MapReduceSinkContext(context, sinkStage, sinkSpec);
    LOG.info("Sink Stage : {}", sinkStage);
    LOG.info("Sink Class : {}", sink.getClass().getName());
    LOG.info("Specifications of Sink : {}", sinkSpec);
    sink.prepareJob(sinkContext);
  }

  private void prepareTransforms(MapReduceContext context, List<ETLStage> transformStages) {
    Job job = context.getHadoopJob();

    //Set list of Transform specifications to be used in the Mapper
    job.getConfiguration().set(Constants.Transform.SPECIFICATIONS,
                               context.getRuntimeArguments().get(Constants.Transform.SPECIFICATIONS));

    //Set the Transform configurations to be used in the initialization in the Mapper
    job.getConfiguration().set(Constants.Transform.CONFIGS, GSON.toJson(transformStages));
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) {
    LOG.info("Batch Run for Adapter {} : {}", context.getRuntimeArguments().get(Constants.ADAPTER_NAME), succeeded);
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceContext> {
    private static final Gson GSON = new Gson();
    private static final Type SPEC_LIST_TYPE = new TypeToken<List<StageSpecification>>() { }.getType();
    private static final Type STAGE_LIST_TYPE = new TypeToken<List<ETLStage>>() { }.getType();

    private TransformExecutor transformExecutor;

    private MapReduceContext mapReduceContext;
    private List<Transform> transforms = Lists.newArrayList();

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      this.mapReduceContext = context;
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      //Get transform class names and specifications from the context.
      String transformSpecs = context.getConfiguration().get(Constants.Transform.SPECIFICATIONS);
      String transformStages = context.getConfiguration().get(Constants.Transform.CONFIGS);

      List<StageSpecification> specificationList = GSON.fromJson(transformSpecs, SPEC_LIST_TYPE);
      List<ETLStage> stageList = GSON.fromJson(transformStages, STAGE_LIST_TYPE);

      LOG.info("Transform Stages : {}", stageList);
      LOG.info("Specifications of Transforms : {}", specificationList);

      Preconditions.checkArgument(stageList.size() == specificationList.size());
      instantiateTransforms(specificationList, stageList);
      transformExecutor = new TransformExecutor(transforms);
    }

    private void instantiateTransforms(List<StageSpecification> specList, List<ETLStage> stageList) {
      for (int i = 0; i < specList.size(); i++) {
        StageSpecification spec = specList.get(i);
        ETLStage stage = stageList.get(i);
        try {
          Transform transform = (Transform) Class.forName(spec.getClassName()).newInstance();
          DefaultTransformContext transformContext = new DefaultTransformContext(spec, stage.getProperties());
          transform.initialize(transformContext);
          transforms.add(transform);
        } catch (ClassNotFoundException e) {
          LOG.error("Unable to load Transform : {}", spec.getClassName(), e);
          Throwables.propagate(e);
        } catch (InstantiationException e) {
          LOG.error("Unable to instantiate Transform : {}", spec.getClassName(), e);
          Throwables.propagate(e);
        } catch (IllegalAccessException e) {
          LOG.error("Error while creating instance of Transform : {}", spec.getClassName(), e);
          Throwables.propagate(e);
        }
      }
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      try {
        for (Map.Entry entry : transformExecutor.runOneIteration(key, value)) {
          context.write(entry.getKey(), entry.getValue());
        }
      } catch (Exception e) {
        LOG.error("Exception thrown in BatchDriver Mapper : {}", e);
        Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      // no-op
    }
  }
}

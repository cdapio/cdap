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
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultEmitter;
import co.cask.cdap.templates.etl.common.DefaultStageConfigurer;
import co.cask.cdap.templates.etl.common.DefaultTransformContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
public class BatchDriver extends AbstractMapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(BatchDriver.class);
  private static final Gson GSON = new Gson();
  private static final JsonParser JSON_PARSER = new JsonParser();

  private BatchSource source;
  private BatchSink sink;

  @Override
  public void configure() {
    setName("BatchDriver");
    setDescription("MapReduce driver for Batch ETL Adapters");
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();

    Preconditions.checkArgument(context.getRuntimeArguments().containsKey(Constants.Source.CLASS_NAME));
    Preconditions.checkArgument(context.getRuntimeArguments().containsKey(Constants.Sink.CLASS_NAME));
    Preconditions.checkArgument(context.getRuntimeArguments().containsKey(Constants.Transform.TRANSFORM_CLASS_LIST));

    String sourceClass = context.getRuntimeArguments().get(Constants.Source.CLASS_NAME);
    String sinkClass = context.getRuntimeArguments().get(Constants.Sink.CLASS_NAME);

    source = (BatchSource) Class.forName(sourceClass).newInstance();
    sink = (BatchSink) Class.forName(sinkClass).newInstance();

    prepareSourceJob(context);
    prepareSinkJob(context);
    prepareTransform(context);

    job.setMapperClass(MapperDriver.class);
    job.setNumReduceTasks(0);
  }

  private void prepareSourceJob(MapReduceContext context) {
    DefaultStageConfigurer sourceConfigurer = new DefaultStageConfigurer(source.getClass());
    source.configure(sourceConfigurer);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, sourceConfigurer.createSpecification());
    source.prepareJob(sourceContext);
  }

  private void prepareSinkJob(MapReduceContext context) {
    DefaultStageConfigurer sinkConfigurer = new DefaultStageConfigurer(sink.getClass());
    sink.configure(sinkConfigurer);
    BatchSinkContext sinkContext = new MapReduceSinkContext(context, sinkConfigurer.createSpecification());
    sink.prepareJob(sinkContext);
  }

  private void prepareTransform(MapReduceContext context) {
    Job job = context.getHadoopJob();
    JsonObject config = JSON_PARSER.parse(context.getRuntimeArguments().get(Constants.CONFIG_KEY)).getAsJsonObject();
    JsonArray transformArray = config.get(Constants.TRANSFORM_KEY).getAsJsonArray();

    //Set list of Transform classes to be used in the Mapper
    job.getConfiguration().set(Constants.Transform.TRANSFORM_CLASS_LIST,
                               context.getRuntimeArguments().get(Constants.Transform.TRANSFORM_CLASS_LIST));

    //Set the properties of the Transforms to be used in the initialization in the Mapper
    job.getConfiguration().set(Constants.Transform.TRANSFORM_PROPERTIES, GSON.toJson(transformArray));
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) {
    //TODO: Get the Adapter Name and include it in the log
    LOG.info("BatchDriver Run {}", succeeded);
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class MapperDriver extends Mapper implements ProgramLifecycle<MapReduceContext> {
    private static final Gson GSON = new Gson();
    private static final JsonParser JSON_PARSER = new JsonParser();
    private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();
    private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
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
      //Get transform class names from the context.
      String transformClassList = context.getConfiguration().get(Constants.Transform.TRANSFORM_CLASS_LIST);
      List<String> classList = GSON.fromJson(transformClassList, STRING_LIST_TYPE);
      JsonArray transformArray = JSON_PARSER.parse(context.getConfiguration().get(
        Constants.Transform.TRANSFORM_PROPERTIES)).getAsJsonArray();
      LOG.info("Transform Classes to be used : {}", classList);
      LOG.info("Properties for Transform Classes : {}", transformArray);
      Preconditions.checkArgument(classList.size() == transformArray.size());
      instatiateTransforms(classList);
      initializeTransforms(transforms, transformArray);
      transformExecutor = new TransformExecutor(transforms);
    }

    private void instatiateTransforms(List<String> classList) {
      for (String transformClass : classList) {
        try {
          Transform transform = (Transform) Class.forName(transformClass).newInstance();
          transforms.add(transform);
        } catch (ClassNotFoundException e) {
          LOG.error("Unable to load Transform : {}; {}", transformClass, e.getMessage(), e);
          Throwables.propagate(e);
        } catch (InstantiationException e) {
          LOG.error("Unable to instatiate Transform : {}; {}", transformClass, e.getMessage(), e);
          Throwables.propagate(e);
        } catch (IllegalAccessException e) {
          LOG.error("Error while creating instance of Transform : {}; {}", transformClass, e.getMessage(), e);
          Throwables.propagate(e);
        }
      }
    }

    private void initializeTransforms(List<Transform> transforms, JsonArray transformArray) {
      for (int i = 0; i < transforms.size(); i++) {
        Transform transform = transforms.get(i);
        JsonObject transformMap = transformArray.get(i).getAsJsonObject();
        JsonObject propertyJson = transformMap.getAsJsonObject(Constants.PROPERTIES_KEY);
        Map<String, String> propertyMap = GSON.fromJson(propertyJson, STRING_MAP_TYPE);
        DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(transform.getClass());
        transform.configure(stageConfigurer);
        DefaultTransformContext transformContext = new DefaultTransformContext(
          stageConfigurer.createSpecification(), propertyMap);
        transform.initialize(transformContext);
      }
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      try {
        DefaultEmitter defaultEmitter = transformExecutor.runOneIteration(key, value);
        for (Map.Entry entry : defaultEmitter) {
          context.write(entry.getKey(), entry.getValue());
        }
        defaultEmitter.reset();
      } catch (Exception e) {
        LOG.error("Exception thrown in BatchDriver Mapper : {}", e.getMessage(), e);
        Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      // no-op
    }
  }
}

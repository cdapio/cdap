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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.etl.api.Condition;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.Constants;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * MapReduce Driver for ETL Preconditions.
 */
public class ETLPreconditionMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLPreconditionMapReduce.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLPreconditionMapReduce.class);

  private static final Gson GSON = new Gson();

  // this is only visible at configure time, not at runtime
  private final ETLBatchConfig config;

  public ETLPreconditionMapReduce(ETLBatchConfig config) {
    this.config = config;
  }

  @Override
  public void configure() {
    setName(NAME);
    setDescription("MapReduce Driver for ETL Batch Applications");

    Resources resources = config.getResources();
    if (resources != null) {
      setMapperResources(resources);
    }
    Resources driverResources = config.getDriverResources();
    if (driverResources != null) {
      setDriverResources(driverResources);
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.STAGE_LOGGING_ENABLED, String.valueOf(config.isStageLoggingEnabled()));
    properties.put(Constants.PRECONDITIONS, GSON.toJson(config.getPreconditions()));
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    Map<String, String> properties = context.getSpecification().getProperties();

    // check preconditions
    co.cask.cdap.etl.api.Preconditions preconditions = GSON.fromJson(
      properties.get(Constants.PRECONDITIONS), co.cask.cdap.etl.api.Preconditions.class);
    long timeoutSec = preconditions.getTimeoutSec();
    long timeStarted = System.currentTimeMillis();
    Queue<Condition> remainingConditions = new LinkedList<>(preconditions.getConditions());
    while (!remainingConditions.isEmpty()) {
      if (System.currentTimeMillis() - timeStarted > timeoutSec) {
        break;
      }
      Condition condition = remainingConditions.peek();
      if (condition.check(context)) {
        remainingConditions.poll();
      }
      Thread.sleep(1000);
    }
    if (!remainingConditions.isEmpty()) {
      throw new RuntimeException("Failed to pass preconditions: " + GSON.toJson(remainingConditions));
    }

    job.setMapperClass(ETLMapper.class);
    job.setNumReduceTasks(0);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    LOG.info("Batch Run finished : succeeded = {}", succeeded);
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceTaskContext<Object, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(ETLMapper.class);
    private static final Gson GSON = new Gson();

    @Override
    public void initialize(MapReduceTaskContext<Object, Object> context) throws Exception {

    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {

    }

    @Override
    public void destroy() {

    }
  }
}

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

package co.cask.cdap.test.template;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.base.Function;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Application Template that uses a worker to write some data to a dataset for testing purposes.
 */
public class WorkflowTemplate extends ApplicationTemplate<WorkflowTemplate.Config> {
  public static final String NAME = "workflowtemplate";
  public static final String INPUT = "workflow.in";
  public static final String OUTPUT = "workflow.out";

  public static class Config {
    private final String functionName;

    public Config(String functionName) {
      this.functionName = functionName;
    }
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(NAME);
    configurer.addWorkflow(new Workflow());
    configurer.addMapReduce(new MapReduce());
    configurer.createDataset(INPUT, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    configurer.createDataset(OUTPUT, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
  }

  @Override
  public void configureAdapter(String name, @Nullable Config config, AdapterConfigurer configurer) throws Exception {
    configurer.usePlugin("function", config.functionName, "func", PluginProperties.builder().build());
    configurer.setSchedule(Schedules.createTimeSchedule("schedule", "description", "* * * * *"));
  }

  /**
   *
   */
  public static class Workflow extends AbstractWorkflow {
    public static final String NAME = "Workflow";
    @Override
    protected void configure() {
      setName(NAME);
      setDescription("Workflow to test Adapter");
      addMapReduce(MapReduce.NAME);
    }
  }

  /**
   *
   */
  public static class MapReduce extends AbstractMapReduce {
    public static final String NAME = "MapReduce";

    @Override
    protected void configure() {
      setName(NAME);
      setInputDataset(INPUT);
      setOutputDataset(OUTPUT);
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(FunctionMapper.class);
      job.setNumReduceTasks(0);
    }
  }

  public static class FunctionMapper extends Mapper<byte[], byte[], byte[], byte[]>
    implements ProgramLifecycle<MapReduceContext> {
    Function<Long, Long> plugin;

    @Override
    public void map(byte[] key, byte[] val, Context context)
      throws IOException, InterruptedException {
      Long newVal = plugin.apply(Bytes.toLong(val));
      context.write(key, Bytes.toBytes(newVal));
    }

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      plugin = context.newPluginInstance("func");
    }

    @Override
    public void destroy() {

    }
  }
}

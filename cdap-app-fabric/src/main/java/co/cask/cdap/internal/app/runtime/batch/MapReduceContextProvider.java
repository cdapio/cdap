/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.Delegators;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.batch.distributed.DistributedMapReduceContextBuilder;
import co.cask.cdap.internal.app.runtime.batch.inmemory.InMemoryMapReduceContextBuilder;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Provides access to MapReduceContext for mapreduce job tasks.
 */
public final class MapReduceContextProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContextProvider.class);

  private final TaskAttemptContext taskContext;
  private final MapReduceMetrics.TaskType type;
  private final MapReduceContextConfig contextConfig;
  private final LocationFactory locationFactory;
  private BasicMapReduceContext context;
  private AbstractMapReduceContextBuilder contextBuilder;

  public MapReduceContextProvider(TaskAttemptContext context, MapReduceMetrics.TaskType type) {
    this.taskContext = context;
    this.type = type;
    this.contextConfig = new MapReduceContextConfig(context.getConfiguration());
    this.locationFactory = new LocalLocationFactory();
    this.contextBuilder = null;
  }

  /**
   * Creates an instance of {@link BasicMapReduceContext} that the {@link co.cask.cdap.app.program.Program} contained
   * inside cannot load program classes. It is used for the cases where only the application specification is needed,
   * but no need to load any class from it.
   */
  public synchronized BasicMapReduceContext get() {
    if (context == null) {
      CConfiguration cConf = contextConfig.getConf();
      context = getBuilder(cConf)
        .build(type,
               contextConfig.getRunId(),
               taskContext.getTaskAttemptID().getTaskID().toString(),
               contextConfig.getLogicalStartTime(),
               contextConfig.getProgramNameInWorkflow(),
               contextConfig.getWorkflowToken(),
               contextConfig.getArguments(),
               contextConfig.getTx(),
               createProgram(contextConfig),
               contextConfig.getInputDataSet(),
               contextConfig.getInputSelection(),
               contextConfig.getOutputDataSet(),
               contextConfig.getAdapterSpec(),
               getPluginInstantiator(cConf)
        );
    }
    return context;
  }

  // TODO: CDAP-3160 : Refactor to remove the need for the stop method below. Provider/Builder classes should not have
  // methods like stop(), finish() or close().
  public synchronized void stop() {
    if (contextBuilder != null) {
      contextBuilder.finish();
    }
  }

  private synchronized AbstractMapReduceContextBuilder getBuilder(CConfiguration conf) {
    if (contextBuilder != null) {
      return contextBuilder;
    }

    if (isLocal(taskContext.getConfiguration())) {
      contextBuilder = new InMemoryMapReduceContextBuilder(conf);
    } else {
      // mrFramework = "yarn" or "classic"
      // if the jobContext is not a TaskAttemptContext, mrFramework should not be yarn.
      contextBuilder = new DistributedMapReduceContextBuilder(
        conf, HBaseConfiguration.create(taskContext.getConfiguration()));
    }
    return contextBuilder;
  }

  /**
   * Helper method to tell if the MR is running in local mode or not. This method doesn't really belongs to this
   * class, but currently there is no better place for it.
   */
  static boolean isLocal(Configuration hConf) {
    String mrFramework = hConf.get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    return MRConfig.LOCAL_FRAMEWORK_NAME.equals(mrFramework);
  }

  private Program createProgram(MapReduceContextConfig contextConfig) {
    Location programLocation;
    if (isLocal(contextConfig.getConfiguration())) {
      // Just create a local location factory. It's for temp usage only as the program location is always absolute.
      programLocation = locationFactory.create(contextConfig.getProgramJarURI());
    } else {
      // In distributed mode, the program jar is localized to the container
      programLocation = locationFactory.create(new File(contextConfig.getProgramJarName()).getAbsoluteFile().toURI());
    }
    try {
      // Use the configuration ClassLoader as the Program ClassLoader
      // In local mode, it is set by the MapReduceRuntimeService
      // In distributed mode, it is set by the MR framework to the ApplicationClassLoader
      return Programs.create(programLocation, contextConfig.getConfiguration().getClassLoader());
    } catch (IOException e) {
      LOG.error("Failed to create program from {}", contextConfig.getProgramJarURI(), e);
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  private PluginInstantiator getPluginInstantiator(CConfiguration cConf) {
    if (contextConfig.getAdapterSpec() == null) {
      return null;
    }

    ClassLoader classLoader = Delegators.getDelegate(cConf.getClassLoader(), MapReduceClassLoader.class);
    if (!(classLoader instanceof MapReduceClassLoader)) {
      throw new IllegalArgumentException("ClassLoader is not an MapReduceClassLoader");
    }
    return ((MapReduceClassLoader) classLoader).getPluginInstantiator();
  }

  /**
   * Returns the {@link ClassLoader} for the MapReduce program. The ClassLoader for MapReduce job is always
   * an {@link MapReduceClassLoader}, which set by {@link MapReduceRuntimeService} in local mode and created by MR
   * framework in distributed mode.
   */
  static ClassLoader getProgramClassLoader(Configuration hConf) {
    ClassLoader classLoader = Delegators.getDelegate(hConf.getClassLoader(), MapReduceClassLoader.class);
    if (!(classLoader instanceof MapReduceClassLoader)) {
      throw new IllegalArgumentException("ClassLoader is not an MapReduceClassLoader");
    }
    return ((MapReduceClassLoader) classLoader).getProgramClassLoader();
  }
}

/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.internal.app.runtime.batch.distributed.DistributedMapReduceContextBuilder;
import co.cask.cdap.internal.app.runtime.batch.inmemory.InMemoryMapReduceContextBuilder;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Provides access to MapReduceContext for mapreduce job tasks.
 */
public final class MapReduceContextProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContextProvider.class);
  private static final ThreadLocal<Program> PROGRAM_THREAD_LOCAL = new ThreadLocal<Program>();

  private final TaskAttemptContext taskContext;
  private final MapReduceMetrics.TaskType type;
  private final MapReduceContextConfig contextConfig;
  private BasicMapReduceContext context;
  private AbstractMapReduceContextBuilder contextBuilder;

  public MapReduceContextProvider(TaskAttemptContext context, MapReduceMetrics.TaskType type) {
    this.taskContext = context;
    this.type = type;
    this.contextConfig = new MapReduceContextConfig(context);
    this.contextBuilder = null;
  }

  /**
   * Creates an instance of {@link BasicMapReduceContext} that the {@link co.cask.cdap.app.program.Program} contained
   * inside cannot load program classes. It is used for the cases where only the application specification is needed,
   * but no need to load any class from it.
   */
  public synchronized BasicMapReduceContext get() {
    if (context == null) {
      CConfiguration conf = contextConfig.getConf();
      context = getBuilder(conf)
        .build(type,
               contextConfig.getRunId(),
               taskContext.getTaskAttemptID().getTaskID().toString(),
               contextConfig.getLogicalStartTime(),
               contextConfig.getWorkflowBatch(),
               contextConfig.getArguments(),
               contextConfig.getTx(),
               createProgram(),
               contextConfig.getInputDataSet(),
               contextConfig.getInputSelection(),
               contextConfig.getOutputDataSet());
    }
    return context;
  }

  private synchronized AbstractMapReduceContextBuilder getBuilder(CConfiguration conf) {
    if (contextBuilder != null) {
      return contextBuilder;
    }

    if (isLocal()) {
      contextBuilder = new InMemoryMapReduceContextBuilder(conf, taskContext);
    } else {
      // mrFramework = "yarn" or "classic"
      // if the jobContext is not a TaskAttemptContext, mrFramework should not be yarn.
      contextBuilder = new DistributedMapReduceContextBuilder(
        conf, HBaseConfiguration.create(taskContext.getConfiguration()), taskContext);
    }
    return contextBuilder;
  }

  private boolean isLocal() {
    String mrFramework = taskContext.getConfiguration().get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    return "local".equals(mrFramework);
  }

  private Program createProgram() {
    // If Program is already created, return it.
    // It is needed so that in distributed mode, there is only one ProgramClassLoader created, even if
    // there are multiple instances of this class being created (one from creating RecordReader/Writer, one from
    // MapperWrapper/ReducerWrapper).

    // For local mode, it doesn't matter than much, but no harm in returning the same program.
    Program program = PROGRAM_THREAD_LOCAL.get();
    if (program != null) {
      return program;
    }

    try {
      if (isLocal()) {
        // Just create a local location factory. It's for temp usage only as the program location is always absolute.
        Location programLocation = new LocalLocationFactory().create(contextConfig.getProgramLocation());
        // In local mode, use the task context classloader to create a new Program instance.
        program = Programs.create(programLocation, taskContext.getConfiguration().getClassLoader());
      } else {
        // In distributed mode, the program is created by expanding the program jar.
        // The program jar is localized to container with the program jar name.
        // It's ok to expand to a temp dir in local directory, as the YARN container will be gone.
        Location programLocation = new LocalLocationFactory()
          .create(new File(contextConfig.getProgramJarName()).getAbsoluteFile().toURI());
        File unpackDir = Files.createTempDir();
        LOG.info("Create Program from {}, expand to {}", programLocation.toURI(), unpackDir);
        program = Programs.createWithUnpack(programLocation, unpackDir,
                                            taskContext.getConfiguration().getClassLoader());
      }
      PROGRAM_THREAD_LOCAL.set(program);
      return program;

    } catch (IOException e) {
      LOG.error("Failed to create program from {}", contextConfig.getProgramLocation(), e);
      throw Throwables.propagate(e);
    }
  }
}

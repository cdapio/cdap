/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.common.output;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An output format that delegates writing to other output formats.
 *
 * It assumes the key is the name of the output that it should delegate to,
 * and the value is a KeyValue containing the actual key and value to send to the delegate.
 */
public class MultiOutputFormat extends OutputFormat<String, KeyValue<Object, Object>> {
  private static final Gson GSON = new Gson();
  private static final Type SET_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, Set<String>>>() { }.getType();
  private static final String NAMES = "cdap.pipeline.multi.names";
  private static final String SINK_OUTPUTS = "cdap.pipeline.multi.sink.outputs";
  private static final String PREFIX = "cdap.pipeline.multi.";
  private static final String FORMAT_SUFFIX = ".format";
  private static final String PROPERTIES_SUFFIX = ".properties";
  private Map<String, OutputFormat<Object, Object>> delegates;

  public static void addOutputs(Configuration hConf, Map<String, OutputFormatProvider> outputs,
                                Map<String, Set<String>> sinkOutputs) {
    hConf.set(NAMES, GSON.toJson(outputs.keySet()));
    hConf.set(SINK_OUTPUTS, GSON.toJson(sinkOutputs));
    for (Map.Entry<String, OutputFormatProvider> entry : outputs.entrySet()) {
      OutputFormatProvider outputFormatProvider = entry.getValue();
      hConf.set(getClassNameKey(entry.getKey()), outputFormatProvider.getOutputFormatClassName());
      hConf.set(getPropertiesKey(entry.getKey()), GSON.toJson(outputFormatProvider.getOutputFormatConfiguration()));
    }
  }

  @Override
  public RecordWriter<String, KeyValue<Object, Object>> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Map<String, Set<String>> sinkOutputs = GSON.fromJson(conf.getRaw(SINK_OUTPUTS), SINK_OUTPUTS_TYPE);
    Map<String, OutputFormat<Object, Object>> delegateFormats = getDelegates(conf);
    /*
        build a map of sinks to writers for that sink.
        For example, if sink1 has outputs o1 and o2, and sink2 has output o3, the map will look like:

          sink1 -> [o1, o2]
          sink2 -> [o3]

        sinks can have multiple outputs if they call addOutput() multiple times in prepareRun().
     */
    Map<String, List<RecordWriter<Object, Object>>> writers = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : sinkOutputs.entrySet()) {
      String sinkName = entry.getKey();
      List<RecordWriter<Object, Object>> sinkWriters = new ArrayList<>();
      for (String sinkOutput : entry.getValue()) {
        OutputFormat<Object, Object> delegate = delegateFormats.get(sinkOutput);
        TaskAttemptContext namedContext = getNamedTaskContext(context, sinkOutput);
        sinkWriters.add(delegate.getRecordWriter(namedContext));
      }
      writers.put(sinkName, sinkWriters);
    }
    return new MultiRecordWriter(writers);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    for (Map.Entry<String, OutputFormat<Object, Object>> entry : getDelegates(context.getConfiguration()).entrySet()) {
      String name = entry.getKey();
      OutputFormat<Object, Object> delegate = entry.getValue();
      JobContext namedContext = getNamedJobContext(context, name);
      delegate.checkOutputSpecs(namedContext);
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    Map<String, OutputCommitter> delegateCommitters = new HashMap<>();
    for (Map.Entry<String, OutputFormat<Object, Object>> entry : getDelegates(context.getConfiguration()).entrySet()) {
      String name = entry.getKey();
      OutputFormat<Object, Object> delegate = entry.getValue();
      TaskAttemptContext namedContext = getNamedTaskContext(context, name);
      delegateCommitters.put(name, delegate.getOutputCommitter(namedContext));
    }
    return new MultiOutputCommitter(delegateCommitters);
  }

  private Map<String, OutputFormat<Object, Object>> getDelegates(Configuration conf) throws IOException {
    if (delegates != null) {
      return delegates;
    }
    delegates = new HashMap<>();
    Set<String> names = GSON.fromJson(conf.get(NAMES), SET_TYPE);
    for (String name : names) {
      String delegateClassName = conf.get(getClassNameKey(name));
      try {
        //noinspection unchecked
        OutputFormat<Object, Object> delegate = (OutputFormat<Object, Object>) conf.getClassLoader()
          .loadClass(delegateClassName)
          .newInstance();
        if (delegate instanceof Configurable) {
          ((Configurable) delegate).setConf(conf);
        }
        delegates.put(name, delegate);
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new IOException("Unable to instantiate delegate input format " + delegateClassName, e);
      }
    }
    return delegates;
  }

  static TaskAttemptContext getNamedTaskContext(TaskAttemptContext context, String name) throws IOException {
    Job job = getNamedJob(context, name);
    return new TaskAttemptContextImpl(job.getConfiguration(), context.getTaskAttemptID(),
                                      new ContextStatusReporter(context));
  }

  static JobContext getNamedJobContext(JobContext context, String name) throws IOException {
    Job job = getNamedJob(context, name);
    return new JobContextImpl(job.getConfiguration(), job.getJobID());
  }

  private static Job getNamedJob(JobContext context, String name) throws IOException {
    Job job = Job.getInstance(context.getConfiguration());
    Configuration conf = job.getConfiguration();
    job.setOutputFormatClass(conf.getClass(getClassNameKey(name), null, OutputFormat.class));

    Map<String, String> properties = GSON.fromJson(conf.getRaw(getPropertiesKey(name)), MAP_TYPE);
    for (Map.Entry<String, String> property : properties.entrySet()) {
      conf.set(property.getKey(), property.getValue());
    }
    return job;
  }

  private static String getClassNameKey(String name) {
    return PREFIX + name + FORMAT_SUFFIX;
  }

  private static String getPropertiesKey(String name) {
    return PREFIX + name + PROPERTIES_SUFFIX;
  }

  /**
   * A StatusReporter that uses the task context to report status.
   */
  private static class ContextStatusReporter extends StatusReporter {
    private final TaskAttemptContext context;

    private ContextStatusReporter(TaskAttemptContext context) {
      this.context = context;
    }

    @Override
    public Counter getCounter(Enum<?> name) {
      return context.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return context.getCounter(group, name);
    }

    @Override
    public void progress() {
      context.progress();
    }

    @Override
    public float getProgress() {
      return context.getProgress();
    }

    @Override
    public void setStatus(String status) {
      context.setStatus(status);
    }
  }
}

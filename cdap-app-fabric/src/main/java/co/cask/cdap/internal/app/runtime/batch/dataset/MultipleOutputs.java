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

package co.cask.cdap.internal.app.runtime.batch.dataset;

import co.cask.cdap.common.lang.Instantiator;
import co.cask.cdap.common.lang.InstantiatorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The MultipleOutputs class simplifies writing output data to multiple outputs.
 * It has been adapted from org.apache.hadoop.mapreduce.lib.output.MultipleOutputs.
 */
public class MultipleOutputs implements Closeable {

  private static final String MULTIPLE_OUTPUTS = "hconf.mapreduce.multipleoutputs";

  private static final String MO_PREFIX = MULTIPLE_OUTPUTS + ".namedOutput.";

  private static final String FORMAT = ".format";
  private static final String KEY = ".key";
  private static final String VALUE = ".value";
  private static final String CONF = ".conf";

  /**
   * Cache for the taskContexts
   */
  private final Map<String, TaskAttemptContext> taskContexts = new HashMap<>();

  // instance code, to be used from Mapper/Reducer code
  private final TaskInputOutputContext context;
  private final Set<String> namedOutputs;
  private final Map<String, RecordWriter<?, ?>> recordWriters;

  /**
   * Checks the existence of a named output within a collection.
   *
   * @throws IllegalArgumentException if the output name is not existing/absent, as appropriate.
   */
  private static void checkNamedOutputName(String namedOutput, Collection<String> namedOutputs, boolean expectToExist) {
    if (!expectToExist && namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput + "' already defined");
    } else if (expectToExist && !namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput + "' not defined");
    }
  }

  // Returns list of channel names.
  static List<String> getNamedOutputsList(JobContext job) {
    Iterable<String> parts =
      Splitter.on(" ").omitEmptyStrings().split(job.getConfiguration().get(MULTIPLE_OUTPUTS, ""));
    return Lists.newArrayList(parts);
  }

  // Returns the named output OutputFormat.
  static Class<? extends OutputFormat> getNamedOutputFormatClass(JobContext job, String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + FORMAT, null, OutputFormat.class);
  }

  // Returns the key class for a named output.
  private static Class<?> getNamedOutputKeyClass(JobContext job, String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + KEY, null, Object.class);
  }

  // Returns the value class for a named output.
  private static Class<?> getNamedOutputValueClass(JobContext job, String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + VALUE, null, Object.class);
  }

  static Map<String, String> getNamedConfigurations(JobContext job, String namedOutput) {
    Map<String, String> namedConf = new HashMap<>();

    String confKeyPrefix = MO_PREFIX + namedOutput + CONF;
    Map<String, String> properties = job.getConfiguration().getValByRegex(confKeyPrefix + ".*");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      namedConf.put(entry.getKey().substring(confKeyPrefix.length()), entry.getValue());
    }
    return namedConf;
  }

  @VisibleForTesting
  static void setNamedConfigurations(Job job, String namedOutput, Map<String, String> namedConf) {
    String confKeyPrefix = MO_PREFIX + namedOutput + CONF;
    for (Map.Entry<String, String> entry : namedConf.entrySet()) {
      job.getConfiguration().set(confKeyPrefix + entry.getKey(), entry.getValue());
    }
  }

  /**
   * Adds a named output for the job.
   *
   * @param job               job to add the named output
   * @param namedOutput       named output name, it has to be a word, letters
   *                          and numbers only, cannot be the word 'part' as
   *                          that is reserved for the default output.
   * @param outputFormatClass name of the OutputFormat class.
   * @param keyClass          key class
   * @param valueClass        value class
   * @param outputConfigs     configurations for the output
   */
  @SuppressWarnings("unchecked")
  public static void addNamedOutput(Job job, String namedOutput, String outputFormatClass,
                                    Class<?> keyClass, Class<?> valueClass, Map<String, String> outputConfigs) {
    checkNamedOutputName(namedOutput, getNamedOutputsList(job), false);
    Configuration conf = job.getConfiguration();
    conf.set(MULTIPLE_OUTPUTS, conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);
    conf.set(MO_PREFIX + namedOutput + FORMAT, outputFormatClass);
    conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
    conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
    setNamedConfigurations(job, namedOutput, outputConfigs);
  }

  /**
   * Creates and initializes multiple outputs support,
   * it should be instantiated in the Mapper/Reducer setup method.
   *
   * @param context the TaskInputOutputContext object
   */
  public MultipleOutputs(TaskInputOutputContext context) {
    this.context = context;
    namedOutputs = Collections.unmodifiableSet(
      new HashSet<>(MultipleOutputs.getNamedOutputsList(context)));
    recordWriters = new HashMap<>();
  }

  /**
   * Write key and value to the namedOutput.
   *
   * @param namedOutput the named output name
   * @param key         the key
   * @param value       the value
   */
  @SuppressWarnings("unchecked")
  public <K, V> void write(String namedOutput, K key, V value) throws IOException, InterruptedException {
    checkNamedOutputName(namedOutput, namedOutputs, true);
    getRecordWriter(namedOutput).write(key, value);
  }

  // by being synchronized MultipleOutputTask can be use with a MultithreadedMapper.
  @SuppressWarnings("unchecked")
  private synchronized RecordWriter getRecordWriter(String namedOutput) throws IOException, InterruptedException {
    // look for record-writer in the cache
    RecordWriter writer = recordWriters.get(namedOutput);

    // If not in cache, create a new one
    if (writer == null) {
      // get the record writer from context output format
      try {
        TaskAttemptContext taskContext = getContext(namedOutput);

        Instantiator<? extends OutputFormat<?, ?>> instantiator =
          new InstantiatorFactory(false).get(TypeToken.of(taskContext.getOutputFormatClass()));
        OutputFormat<?, ?> outputFormat = instantiator.create();

        writer = outputFormat.getRecordWriter(taskContext);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      // add the record-writer to the cache
      recordWriters.put(namedOutput, writer);
    }
    return writer;
  }

  // Create a taskAttemptContext for the named output with
  // output format and output key/value types put in the context
  private synchronized TaskAttemptContext getContext(String nameOutput) throws IOException {

    TaskAttemptContext taskContext = taskContexts.get(nameOutput);

    if (taskContext != null) {
      return taskContext;
    }

    taskContext = getNamedTaskContext(context, nameOutput);
    taskContexts.put(nameOutput, taskContext);
    return taskContext;
  }

  static TaskAttemptContext getNamedTaskContext(TaskAttemptContext context, String namedOutput) throws IOException {
    Job job = getNamedJob(context, namedOutput);
    return new TaskAttemptContextImpl(job.getConfiguration(),
                                      context.getTaskAttemptID(), new WrappedStatusReporter(context));
  }

  static JobContext getNamedJobContext(JobContext context, String namedOutput) throws IOException {
    Job job = getNamedJob(context, namedOutput);
    return new JobContextImpl(job.getConfiguration(), job.getJobID());
  }

  private static Job getNamedJob(JobContext context, String namedOutput) throws IOException {
    // The following trick leverages the instantiation of a record writer via
    // the job thus supporting arbitrary output formats.
    Job job = new Job(context.getConfiguration());
    job.setOutputFormatClass(getNamedOutputFormatClass(context, namedOutput));
    job.setOutputKeyClass(getNamedOutputKeyClass(context, namedOutput));
    job.setOutputValueClass(getNamedOutputValueClass(context, namedOutput));

    Configuration conf = job.getConfiguration();
    Map<String, String> namedConfigurations = getNamedConfigurations(context, namedOutput);
    for (Map.Entry<String, String> entry : namedConfigurations.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return job;
  }

  private static class WrappedStatusReporter extends StatusReporter {

    TaskAttemptContext context;

    public WrappedStatusReporter(TaskAttemptContext context) {
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

  /**
   * Closes all the opened outputs.
   * This should be called from cleanup method of map/reduce task.
   */
  @SuppressWarnings("unchecked")
  public void close() {
    closeRecordWriters(recordWriters.values(), context);
  }

  /**
   * Closes a collection of RecordWriters, suppressing any exceptions until close is called on each of them.
   *
   * @param recordWriters The Collection of RecordWriters to close
   * @param context The context to pass during close of each RecordWriter
   */
  public static void closeRecordWriters(Iterable<RecordWriter<?, ?>> recordWriters,
                                        TaskAttemptContext context) {
    RuntimeException ex = null;
    for (RecordWriter writer : recordWriters) {
      try {
        writer.close(context);
      } catch (IOException | InterruptedException e) {
        if (ex == null) {
          ex = new RuntimeException(e);
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }
}

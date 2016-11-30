/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset.output;

import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.conf.ConfigurationUtil;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.proto.id.EntityId;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

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
  private static final String PREFIXED_CONF_PREFIX = "hconf.named.";

  private static final String MO_PREFIX = MULTIPLE_OUTPUTS + ".namedOutput.";

  private static final String FORMAT = ".format";
  private static final String KEY = ".key";
  private static final String VALUE = ".value";

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
      // this shouldn't happen, because it is already protected against in BasicMapReduceContext#addOutput
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

  /**
   * Adds a named output for the job.
   *
   * @param job               job to add the named output
   * @param namedOutput       named output name, it has to be a word, letters
   *                          and numbers only (alphanumeric)
   * @param outputFormatClass name of the OutputFormat class.
   * @param keyClass          key class
   * @param valueClass        value class
   * @param outputConfigs     configurations for the output
   */
  @SuppressWarnings("unchecked")
  public static void addNamedOutput(Job job, String namedOutput, String outputFormatClass,
                                    Class<?> keyClass, Class<?> valueClass, Map<String, String> outputConfigs) {
    assertValidName(namedOutput);
    checkNamedOutputName(namedOutput, getNamedOutputsList(job), false);
    Configuration conf = job.getConfiguration();
    conf.set(MULTIPLE_OUTPUTS, conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);
    conf.set(MO_PREFIX + namedOutput + FORMAT, outputFormatClass);
    conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
    conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
    ConfigurationUtil.setNamedConfigurations(conf, computePrefixName(namedOutput), outputConfigs);
  }

  private static String computePrefixName(String outputName) {
    // suffix the outputName with an '.', so that one outputName being a prefix of another outputName doesn't cause
    // conflicts when scanning for properties
    return PREFIXED_CONF_PREFIX + outputName + ".";
  }

  private static void assertValidName(String name) {
    // use the same check as used on datasets when they're created, since the output name can be any dataset name
    Preconditions.checkArgument(EntityId.isValidId(name),
                                "Name '%s' must consist only of ASCII letters, numbers, _, or -.", name);
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
      TaskAttemptContext taskContext = getContext(namedOutput);

      Class<? extends OutputFormat<?, ?>> outputFormatClass;
      try {
        outputFormatClass = taskContext.getOutputFormatClass();
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      ClassLoader outputFormatClassLoader = outputFormatClass.getClassLoader();
      // This is needed in case the OutputFormat's classloader conflicts with the program classloader (for example,
      // TableOutputFormat).
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(outputFormatClassLoader);

      try {
        // We use ReflectionUtils to instantiate the OutputFormat, because it also calls setConf on the object, if it
        // is a org.apache.hadoop.conf.Configurable.
        OutputFormat<?, ?> outputFormat =
          ReflectionUtils.newInstance(outputFormatClass, taskContext.getConfiguration());
        writer = new MeteredRecordWriter<>(outputFormat.getRecordWriter(taskContext), context);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
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
    Job job = Job.getInstance(context.getConfiguration());
    job.setOutputFormatClass(getNamedOutputFormatClass(context, namedOutput));
    job.setOutputKeyClass(getNamedOutputKeyClass(context, namedOutput));
    job.setOutputValueClass(getNamedOutputValueClass(context, namedOutput));

    Configuration conf = job.getConfiguration();
    Map<String, String> namedConfigurations = ConfigurationUtil.getNamedConfigurations(context.getConfiguration(),
                                                                                       computePrefixName(namedOutput));
    ConfigurationUtil.setAll(namedConfigurations, conf);
    return job;
  }

  /**
   * Wraps RecordWriter to increment output counters.
   *
   * Normally, the user calls context#write(key, value) - context in this case is a Hadoop class, which automatically
   * increments the counter as well as writing the record.
   * In the case of multiple outputs, the user calls context#write(outputName, key, value) - in this case, the context
   * is a CDAP class, and this doesn't at all translate into a call to Hadoop's context#write. Because of that, the
   * metrics for output records aren't automatically incremented.
   */
  private static class MeteredRecordWriter<K, V> extends RecordWriter<K, V> {
    private final RecordWriter<K, V> writer;
    private final String groupName;
    private final String counterName;
    private final TaskInputOutputContext context;

    MeteredRecordWriter(RecordWriter<K, V> writer, TaskInputOutputContext context) {
      this.writer = writer;
      this.context = context;
      this.groupName = TaskCounter.class.getName();
      this.counterName = getCounterName(context);
    }

    public void write(K key, V value) throws IOException, InterruptedException {
      context.getCounter(groupName, counterName).increment(1);
      writer.write(key, value);
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      writer.close(context);
    }

    private String getCounterName(TaskInputOutputContext context) {
      MapReduceMetrics.TaskType taskType = MapReduceMetrics.TaskType.from(context.getTaskAttemptID().getTaskType());
      switch (taskType) {
        case Mapper:
          return TaskCounter.MAP_OUTPUT_RECORDS.name();
        case Reducer:
          return TaskCounter.REDUCE_OUTPUT_RECORDS.name();
        default:
          throw new IllegalArgumentException("Illegal task type: " + taskType);
      }
    }
  }

  private static class WrappedStatusReporter extends StatusReporter {

    TaskAttemptContext context;

    WrappedStatusReporter(TaskAttemptContext context) {
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
  public void close() {
    closeRecordWriters(recordWriters, taskContexts);
  }

  /**
   * Closes a collection of RecordWriters, suppressing any exceptions until close is called on each of them.
   * @param recordWriters The map of RecordWriters to close
   * @param taskContexts The map of context to pass during close of each RecordWriter
   * @param <K> type of the key for recordWriter map
   */
  public static <K> void closeRecordWriters(Map<K, RecordWriter<?, ?>> recordWriters,
                                            Map<K, TaskAttemptContext> taskContexts) {
    RuntimeException ex = null;
    for (Map.Entry<K, RecordWriter<?, ?>> entry : recordWriters.entrySet()) {
      try {
        RecordWriter<?, ?> recordWriter = entry.getValue();
        recordWriter.close(taskContexts.get(entry.getKey()));
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

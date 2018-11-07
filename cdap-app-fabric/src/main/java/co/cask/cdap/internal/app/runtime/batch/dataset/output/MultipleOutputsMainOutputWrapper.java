/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.internal.app.runtime.batch.BasicOutputFormatProvider;
import co.cask.cdap.internal.app.runtime.batch.MainOutputCommitter;
import co.cask.cdap.internal.app.runtime.batch.dataset.UnsupportedOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * OutputFormat that wraps a root OutputFormat and provides an OutputFormatCommitter that delegates to multiple
 * preconfigured OutputFormatCommitters. By design, this is the OutputFormat configured for all MapReduce jobs
 * being executed by CDAP. See MapReduceRuntimeService#setOutputsIfNeeded.
 *
 * @param <K> Type of key
 * @param <V> Type of value
 */
public class MultipleOutputsMainOutputWrapper<K, V> extends OutputFormat<K, V> {
  private static final String ROOT_OUTPUT_FORMAT =
    MultipleOutputsMainOutputWrapper.class.getCanonicalName() + ".rootOutputFormat";
  private OutputFormat<K, V> innerFormat;
  private OutputCommitter committer;

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    OutputFormat<K, V> rootOutputFormat = getRootOutputFormat(job);
    List<String> namedOutputsList = MultipleOutputs.getNamedOutputsList(job);
    if (namedOutputsList.size() == 1) {
      // CDAP-14524 remove prefixes added before each key. We only need this for single output because for single
      // output rootOutputFormat is actual output format.
      TaskAttemptContext namedContext = MultipleOutputs.getNamedTaskContext(job, namedOutputsList.get(0));
      return rootOutputFormat.getRecordWriter(namedContext);
    }
    return rootOutputFormat.getRecordWriter(job);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    for (String name : MultipleOutputs.getNamedOutputsList(context)) {
      Class<? extends OutputFormat> namedOutputFormatClass =
        MultipleOutputs.getNamedOutputFormatClass(context, name);
      JobContext namedContext = MultipleOutputs.getNamedJobContext(context, name);

      OutputFormat<K, V> outputFormat =
        ReflectionUtils.newInstance(namedOutputFormatClass, namedContext.getConfiguration());

      outputFormat.checkOutputSpecs(namedContext);
    }
  }

  // the root OutputFormat is used only for writing, not for checking output specs or committing of the output
  // because the root is also in the delegates, which check the output spec and commit the output.
  private OutputFormat<K, V> getRootOutputFormat(JobContext context) throws InvalidJobConfException {
    if (innerFormat == null) {
      Configuration conf = context.getConfiguration();
      @SuppressWarnings("unchecked")
      Class<? extends OutputFormat<K, V>> c =
        (Class<? extends OutputFormat<K, V>>) conf.getClass(ROOT_OUTPUT_FORMAT, null, OutputFormat.class);
      if (c == null) {
        throw new InvalidJobConfException("The job configuration does not contain required property: "
                                            + ROOT_OUTPUT_FORMAT);
      }
      innerFormat = ReflectionUtils.newInstance(c, conf);
    }
    return innerFormat;
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    if (committer == null) {
      // use a linked hash map: it preserves the order of insertion, so the output committers are called in the
      // same order as outputs were added. This makes multi-output a little more predictable (and testable).
      Map<String, OutputCommitter> delegates = new LinkedHashMap<>();

      List<String> namedOutputsList = MultipleOutputs.getNamedOutputsList(context);
      // if there is only 1 output configured, it is the same as the root OutputFormat, so no need to have it also in
      // the delegates; otherwise, its methods would get called more than expected.
      // If more than 1 outputs are configured, then the root OutputCommitter is a NullOutputCommitter (no-op).
      // See MapReduceRuntimeService#setOutputsIfNeeded.
      if (namedOutputsList.size() == 1) {
        // CDAP-14524 Get context without prefixes for single output. This is needed becausez
        TaskAttemptContext namedContext = MultipleOutputs.getNamedTaskContext(context, namedOutputsList.get(0));
        committer = new MainOutputCommitter(getRootOutputFormat(namedContext).getOutputCommitter(namedContext),
                                            delegates, namedContext);
      } else {
        for (String name : namedOutputsList) {
          TaskAttemptContext namedContext = MultipleOutputs.getNamedTaskContext(context, name);
          Class<? extends OutputFormat> namedOutputFormatClass =
            MultipleOutputs.getNamedOutputFormatClass(context, name);

          OutputFormat<K, V> outputFormat =
            ReflectionUtils.newInstance(namedOutputFormatClass, namedContext.getConfiguration());
          delegates.put(name, outputFormat.getOutputCommitter(namedContext));
        }
        // return a MultipleOutputsCommitter that commits for the root output format
        // as well as all delegate outputformats
        committer = new MainOutputCommitter(getRootOutputFormat(context).getOutputCommitter(context),
                                            delegates, context);
      }
    }
    return committer;
  }

  /**
   * Sets output formats and corresponding properties.
   *
   * @param job hadoop job on which configurations will be set
   * @param outputsMap list of outputs
   */
  public static void setOutputs(Job job, List<ProvidedOutput> outputsMap)
    throws ClassNotFoundException {
    OutputFormatProvider rootOutputFormatProvider;
    rootOutputFormatProvider = getRootOutputFormatProvider(job, outputsMap);

    // Set root outputformat and its configuration for the Hadoop job.
    job.getConfiguration().set(ROOT_OUTPUT_FORMAT, rootOutputFormatProvider.getOutputFormatClassName());
    for (Map.Entry<String, String> confEntry : rootOutputFormatProvider.getOutputFormatConfiguration().entrySet()) {
      job.getConfiguration().set(confEntry.getKey(), confEntry.getValue());
    }

    for (ProvidedOutput output : outputsMap) {
      MultipleOutputs.addNamedOutput(job, output.getOutput().getAlias(), output.getOutputFormatClassName(),
                                     job.getOutputKeyClass(), job.getOutputValueClass(),
                                     output.getOutputFormatConfiguration());

    }
  }

  private static OutputFormatProvider getRootOutputFormatProvider(Job job, List<ProvidedOutput> outputsMap)
    throws ClassNotFoundException {
    OutputFormatProvider rootOutputFormatProvider;
    // There are 3 cases possible:
    // [1] output map is empty: user is not going through our APIs to add output; propagate the job's output format
    // [2] output map has single output: provided output format is set as the root OutputFormat
    // [3] output map has multiple outputs: RootOutputFormat is UnsupportedOutputFormat which has RecordWriter that
    // does not support writing. The OutputCommitter is effectively a no-op, as it runs as the RootOutputCommitter
    // in MultipleOutputsCommitter.

    if (outputsMap.isEmpty()) {
      rootOutputFormatProvider = new BasicOutputFormatProvider(job.getOutputFormatClass().getName(),
                                                               Collections.emptyMap());
    } else if (outputsMap.size() == 1) {
      rootOutputFormatProvider = outputsMap.get(0).getOutputFormatProvider();
    } else {
      rootOutputFormatProvider = new BasicOutputFormatProvider(UnsupportedOutputFormat.class.getName(),
                                                               Collections.emptyMap());
    }
    return rootOutputFormatProvider;
  }
}

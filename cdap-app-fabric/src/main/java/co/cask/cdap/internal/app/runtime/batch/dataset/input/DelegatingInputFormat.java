/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import co.cask.cdap.common.conf.ConfigurationUtil;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An {@link InputFormat} that delegates behavior of InputFormat to multiple other InputFormats.
 *
 * @see MultipleInputs#addInput(Job, String, String, Map, Class)
 *
 * @param <K> Type of key
 * @param <V> Type of value
 */
public class DelegatingInputFormat<K, V> extends InputFormat<K, V> {

  @SuppressWarnings("unchecked")
  public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
    List<InputSplit> splits = new ArrayList<>();
    Map<String, MultipleInputs.MapperInput> mapperInputMap = MultipleInputs.getInputMap(job.getConfiguration());

    for (Map.Entry<String, MultipleInputs.MapperInput> mapperInputEntry : mapperInputMap.entrySet()) {
      String inputName = mapperInputEntry.getKey();
      MultipleInputs.MapperInput mapperInput = mapperInputEntry.getValue();
      String mapperClassName = mapperInput.getMapperClassName();
      Job jobCopy = new Job(job.getConfiguration());
      Configuration confCopy = jobCopy.getConfiguration();

      // set configuration specific for this input onto the jobCopy
      ConfigurationUtil.setAll(mapperInput.getInputFormatConfiguration(), confCopy);

      Class<?> inputFormatClass = confCopy.getClassByNameOrNull(mapperInput.getInputFormatClassName());
      Preconditions.checkNotNull(inputFormatClass, "Class could not be found: ", mapperInput.getInputFormatClassName());
      InputFormat inputFormat = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, confCopy);
      //some input format need a jobId to getSplits
      jobCopy.setJobID(new JobID(inputName, inputName.hashCode()));
      // Get splits for each input path and tag with InputFormat
      // and Mapper types by wrapping in a TaggedInputSplit.
      List<InputSplit> formatSplits = inputFormat.getSplits(jobCopy);
      for (InputSplit split : formatSplits) {
        splits.add(new TaggedInputSplit(inputName, split, confCopy, mapperInput.getInputFormatConfiguration(),
                                        inputFormat.getClass(), mapperClassName));
      }
    }
    return splits;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context) throws IOException, InterruptedException {
    TaggedInputSplit taggedInputSplit = (TaggedInputSplit) split;
    ConfigurationUtil.setAll((taggedInputSplit).getInputConfigs(), context.getConfiguration());
    return new DelegatingRecordReader<>(taggedInputSplit, context);
  }
}

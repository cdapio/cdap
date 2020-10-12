/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.preview;

import io.cdap.cdap.etl.batch.DelegatingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A wrapper around another input format, that limits the amount of data read.
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public class LimitingInputFormat<K, V> extends DelegatingInputFormat<K, V> {

  static final String MAX_RECORDS = "io.cdap.pipeline.preview.max.records";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    int maxRecords = conf.getInt(MAX_RECORDS, 100);
    List<InputSplit> splits = super.getSplits(context);
    return Collections.singletonList(new LimitingInputSplit(conf, splits, maxRecords));
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new LimitingRecordReader<>(getDelegate(context.getConfiguration()));
  }
}

/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Custom inputformat to apply wrangler Directive for explore
 */
public class WranglerInputFormat extends InputFormat<Void, StructuredRecord> {
  private TextInputFormat delegate;

  public WranglerInputFormat() {
    this.delegate = new TextInputFormat();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return delegate.getSplits(context);
  }

  @Override
  public RecordReader<Void, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new WranglerRecordReader(new LineRecordReader());
  }
}

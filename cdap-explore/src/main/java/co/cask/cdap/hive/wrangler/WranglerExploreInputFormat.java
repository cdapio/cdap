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

package co.cask.cdap.hive.wrangler;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;

/**
 *
 */
public class WranglerExploreInputFormat implements InputFormat<Void, StructuredRecordWritable> {
  private TextInputFormat delegate;

  public WranglerExploreInputFormat() {
    this.delegate = new TextInputFormat();
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    delegate.configure(job);
    return delegate.getSplits(job, numSplits);
  }

  @Override
  public RecordReader<Void, StructuredRecordWritable> getRecordReader(InputSplit genericSplit, JobConf job,
                                                                      Reporter reporter) throws IOException {
    return new WranglerRecordWritableReader(job, new LineRecordReader(job, (FileSplit) genericSplit, null));
  }
}

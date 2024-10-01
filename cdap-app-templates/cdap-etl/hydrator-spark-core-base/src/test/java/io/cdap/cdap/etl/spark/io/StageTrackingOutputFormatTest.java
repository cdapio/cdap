/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.io;

import io.cdap.cdap.api.exception.WrappedStageException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.junit.Test;

/**
 * Unit tests for {@link StageTrackingOutputFormat} class.
 */
public class StageTrackingOutputFormatTest {

  @Test(expected = WrappedStageException.class)
  public void testMissingDelegate() throws IOException {
    Configuration hConf = new Configuration();
    Job job = Job.getInstance(hConf);
    StageTrackingOutputFormat outputFormat = new StageTrackingOutputFormat();
    outputFormat.checkOutputSpecs(job);
  }

  @Test(expected = WrappedStageException.class)
  public void testSelfDelegate() throws IOException {
    Configuration hConf = new Configuration();
    hConf.setClass(StageTrackingOutputFormat.DELEGATE_CLASS_NAME, StageTrackingOutputFormat.class,
      OutputFormat.class);
    Job job = Job.getInstance(hConf);
    StageTrackingOutputFormat outputFormat = new StageTrackingOutputFormat();
    outputFormat.checkOutputSpecs(job);
  }
}

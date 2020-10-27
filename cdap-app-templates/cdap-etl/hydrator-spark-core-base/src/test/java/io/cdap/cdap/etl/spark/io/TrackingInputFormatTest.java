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

package io.cdap.cdap.etl.spark.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/**
 * Unit tests for {@link TrackingInputFormat} class.
 */
public class TrackingInputFormatTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  
  @Test
  public void testDelegate() throws IOException, InterruptedException {
    File inputDir = TEMP_FOLDER.newFolder();
    Files.createFile(inputDir.toPath().resolve("test"));

    Configuration hConf = new Configuration();
    hConf.setClass(TrackingInputFormat.DELEGATE_CLASS_NAME, TextInputFormat.class, InputFormat.class);

    Job job = Job.getInstance(hConf);
    TextInputFormat.addInputPath(job, new Path(inputDir.toURI()));

    TrackingInputFormat inputFormat = new TrackingInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(job);
    Assert.assertEquals(1, splits.size());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testMissingDelegate() throws IOException, InterruptedException {
    Configuration hConf = new Configuration();
    Job job = Job.getInstance(hConf);
    TrackingInputFormat inputFormat = new TrackingInputFormat();
    inputFormat.getSplits(job);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testSelfDelegate() throws IOException, InterruptedException {
    Configuration hConf = new Configuration();
    hConf.setClass(TrackingInputFormat.DELEGATE_CLASS_NAME, TrackingInputFormat.class, InputFormat.class);

    Job job = Job.getInstance(hConf);
    TrackingInputFormat inputFormat = new TrackingInputFormat();
    inputFormat.getSplits(job);
  }
}

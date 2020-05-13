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

package io.cdap.cdap.etl.batch.preview;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the {@link LimitingInputFormat}.
 */
@RunWith(Parameterized.class)
public class LimitingInputFormatTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Parameterized.Parameters(name = "{index} : numOfFiles = {0} maxRecords = {1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
      new Object[] { 0, 5 },
      new Object[] { 1, 5 },
      new Object[] { 1, 25 },
      new Object[] { 1, 151 },
      new Object[] { 1, 2000 },
      new Object[] { 10, 5 },
      new Object[] { 10, 25 },
      new Object[] { 10, 151 },
      new Object[] { 10, 2000 }
    );
  }

  private final int numOfFiles;
  private final int maxRecords;
  private File inputDir;

  public LimitingInputFormatTest(int numOfFiles, int maxRecords) {
    this.numOfFiles = numOfFiles;
    this.maxRecords = maxRecords;
  }

  @Before
  public void generateInput() throws IOException {
    File inputDir = TEMP_FOLDER.newFolder();

    // Generate N files, each with 100 lines
    for (int i = 0; i < numOfFiles; i++) {
      File file = new File(inputDir, "file" + i + ".txt");
      try (BufferedWriter writer = Files.newBufferedWriter(file.toPath())) {
        for (int j = 0; j < 100; j++) {
          writer.write(file.getName());
          writer.newLine();
        }
      }
    }
    this.inputDir = inputDir;
  }

  @Test
  public void testSplits() throws IOException, InterruptedException {
    Configuration hConf = new Configuration();
    hConf.set(LimitingInputFormat.DELEGATE_CLASS_NAME, TextInputFormat.class.getName());
    hConf.setInt(LimitingInputFormat.MAX_RECORDS, maxRecords);

    // Create splits from the given directory
    Job job = Job.getInstance(hConf);
    job.setJobID(new JobID("test", 0));
    FileInputFormat.addInputPath(job, new Path(inputDir.toURI()));

    LimitingInputFormat<LongWritable, Text> inputFormat = new LimitingInputFormat<>();
    inputFormat.setConf(hConf);

    List<InputSplit> splits = inputFormat.getSplits(job);
    Assert.assertEquals(1, splits.size());

    InputSplit split = splits.get(0);

    // Serialize the splits and deserialize it back. This is to verify the serialization.
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    SerializationFactory serializationFactory = new SerializationFactory(hConf);

    Serializer serializer = serializationFactory.getSerializer(split.getClass());
    serializer.open(bos);
    //noinspection unchecked
    serializer.serialize(split);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    Deserializer deserializer = serializationFactory.getDeserializer(split.getClass());
    deserializer.open(bis);
    //noinspection unchecked
    split = (InputSplit) deserializer.deserialize(null);

    TaskID taskId = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptContext taskContext = new TaskAttemptContextImpl(hConf, new TaskAttemptID(taskId, 0));

    Map<String, Integer> recordsPerFiles = new HashMap<>();
    try (RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, taskContext)) {
      reader.initialize(split, taskContext);
      while (reader.nextKeyValue()) {
        String fileName = reader.getCurrentValue().toString();
        int record = recordsPerFiles.getOrDefault(fileName, 0);
        recordsPerFiles.put(fileName, ++record);
      }
    }

    int recordsPerFile = numOfFiles == 0 ? 0 : (maxRecords + numOfFiles - 1) / numOfFiles;
    int filesToRead = recordsPerFile == 0 ? 0 : (maxRecords + recordsPerFile - 1) / recordsPerFile;
    Assert.assertEquals(filesToRead, recordsPerFiles.size());
  }
}

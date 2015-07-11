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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.spark.AbstractSpark;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * A dummy app with spark program which counts the characters in a string
 */
public class ScalaSparkAppUsingFileSet extends AbstractApplication {
  @Override
  public void configure() {
    try {
      setName("SparkAppUsingFileSet");
      setDescription("Application with Spark program using fileset input/output");
      createDataset("fs", FileSet.class, FileSetProperties.builder()
        .setInputFormat(MyTextFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
      createDataset("pfs", PartitionedFileSet.class, PartitionedFileSetProperties.builder()
        .setPartitioning(Partitioning.builder().addStringField("x").build())
        .setInputFormat(MyTextFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
      createDataset("tpfs", TimePartitionedFileSet.class, FileSetProperties.builder()
        .setInputFormat(MyTextFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
      addSpark(new CharCountSpecification());
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  public static final class CharCountSpecification extends AbstractSpark {
    @Override
    public void configure() {
      setName("SparkCharCountProgram");
      setDescription("Use Objectstore dataset as input job");
      setMainClass(ScalaFileCountProgram.class);
    }
  }

  /**
   * An input format that delegates to TextInputFormat. It is defined in the application and requires
   * the Spark runtime to use the program class loader to load the class.
   */
  public static final class MyTextFormat extends InputFormat<Long, String> {

    TextInputFormat delegate = new TextInputFormat();

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      return delegate.getSplits(context);
    }

    @Override
    public RecordReader<Long, String> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new MyRecordReader(delegate.createRecordReader(split, context));
    }
  }

  public static class MyRecordReader extends RecordReader<Long, String> {

    private RecordReader<LongWritable, Text> delegate;

    public MyRecordReader(RecordReader<LongWritable, Text> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
      delegate.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
    }

    @Override
    public Long getCurrentKey() throws IOException, InterruptedException {
      LongWritable writable = delegate.getCurrentKey();
      return writable == null ? null : writable.get();
    }

    @Override
    public String getCurrentValue() throws IOException, InterruptedException {
      Text text = delegate.getCurrentValue();
      return text == null ? null : text.toString();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return delegate.getProgress();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}

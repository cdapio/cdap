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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.exception.WrappedException;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class WrappedInputFormat<K, V> extends InputFormat<K, V> {
  private final InputFormat<K, V> inputFormat;
  private final String stageName;

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    try {
      return inputFormat.getSplits(jobContext);
    } catch (Exception e) {
      if (stageName != null) {
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    try {
      return new WrappedRecordReader<>(inputFormat.createRecordReader(inputSplit,
          taskAttemptContext), stageName);
    } catch (Exception e) {
      if (stageName != null) {
        throw new WrappedException(e, stageName);
      }
      throw e;
    }
  }

  /**
   * Returns the delegating {@link InputFormat} based on the current configuration.
   *
   * @param classLoader the {@link ClassLoader} for loading input format
   * @param inputFormatClassName the name of {@link InputFormat} class
   * @throws IOException if failed to instantiate the input format class
   */
  public WrappedInputFormat(ClassLoader classLoader, String inputFormatClassName,
      String stageName) throws IOException {
    this.stageName = stageName;
    if (inputFormatClassName == null) {
      throw new IllegalArgumentException("Missing configuration for the InputFormat to use");
    }
    if (inputFormatClassName.equals(getClass().getName())) {
      throw new IllegalArgumentException("Cannot delegate InputFormat to the same class");
    }
    try {
      //noinspection unchecked
      @SuppressWarnings("unchecked")
      Class<InputFormat<K, V>> inputFormatClass = (Class<InputFormat<K, V>>) classLoader.loadClass(
          inputFormatClassName);
      this.inputFormat = inputFormatClass.newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new IOException(
          String.format("Unable to instantiate delegate input format %s", inputFormatClassName), e);
    }
  }
}

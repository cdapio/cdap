/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.stream;

import co.cask.cdap.api.stream.StreamEventDecoder;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Stream mapreduce input format.
 *
 * @param <K> type of the key
 * @param <V> type of the value
 */
public class StreamInputFormat<K, V> extends InputFormat<K, V> {
  private static final StreamInputSplitFactory<InputSplit> splitFactory = new StreamInputSplitFactory<InputSplit>() {
    @Override
    public InputSplit createSplit(Path path, Path indexPath, long startTime, long endTime,
                                  long start, long length, @Nullable String[] locations) {
      return new StreamInputSplit(path, indexPath, startTime, endTime, start, length, locations);
    }
  };

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return StreamInputFormatConfigurer.getSplits(context.getConfiguration(), getCurrentTime(), splitFactory);
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new StreamRecordReader(createStreamEventDecoder(context.getConfiguration()));
  }

  protected long getCurrentTime() {
    return System.currentTimeMillis();
  }

  @SuppressWarnings("unchecked")
  protected StreamEventDecoder<K, V> createStreamEventDecoder(Configuration conf) {
    Class<? extends StreamEventDecoder> decoderClass = StreamInputFormatConfigurer.getDecoderClass(conf);
    Preconditions.checkNotNull(decoderClass, "Failed to load stream event decoder.");
    try {
      return decoderClass.newInstance();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

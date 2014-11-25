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

package co.cask.cdap.hive.stream;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamInputSplitFactory;
import co.cask.cdap.data.stream.StreamInputSplitFinder;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.hive.context.ContextManager;
import com.google.common.base.Throwables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Stream input format for use in hive queries and only hive queries. Will not work outside of hive.
 */
public class HiveStreamInputFormat implements InputFormat<Void, ObjectWritable> {
  private static final StreamInputSplitFactory<InputSplit> splitFactory = new StreamInputSplitFactory<InputSplit>() {
    @Override
    public InputSplit createSplit(Path path, Path indexPath, long startTime, long endTime,
                                  long start, long length, @Nullable String[] locations) {
      return new StreamInputSplit(path, indexPath, startTime, endTime, start, length, locations);
    }
  };

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    // right before this method is called by hive, hive copies everything that the storage handler put in the properties
    // map in it's configureTableJobProperties method into the job conf. We put the stream name in there so that
    // we can derive the stream path and properties from it.
    // this MUST be done in the input format and not in StreamSerDe's initialize method because we have no control
    // over when initialize is called. If we set job conf settings there, the settings for one stream get clobbered
    // by the settings for another stream if a join over streams is being performed.
    String streamName = conf.get(Constants.Explore.STREAM_NAME);
    StreamInputSplitFinder<InputSplit> splitFinder = getSplitFinder(conf, streamName);
    try {
      List<InputSplit> splits = splitFinder.getSplits(conf);
      InputSplit[] splitArray = new InputSplit[splits.size()];
      int i = 0;
      for (InputSplit split : splits) {
        splitArray[i] = split;
        i++;
      }
      return splitArray;
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public RecordReader<Void, ObjectWritable> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
    throws IOException {
    return new StreamRecordReader(split, conf);
  }

  private StreamInputSplitFinder<InputSplit> getSplitFinder(JobConf conf, String streamName) throws IOException {
    // first get the context we are in
    ContextManager.Context context = ContextManager.getContext(conf);
    // get the stream admin from the context, which will let us get stream information such as the path
    StreamAdmin streamAdmin = context.getStreamAdmin();
    StreamConfig streamConfig = streamAdmin.getConfig(streamName);
    // make sure we get the current generation so we don't read events that occurred before a truncate.
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));

    // TODO: examine query where clause to get a better start and end time based on timestamp filters.
    // the conf contains a 'hive.io.filter.expr.serialized' key which contains the serialized form of ExprNodeDesc
    URI path = streamPath.toURI();
    long startTime = Math.max(0L, System.currentTimeMillis() - streamConfig.getTTL());
    return StreamInputSplitFinder.builder(path)
      .setStartTime(startTime)
      .build(splitFactory);
  }
}

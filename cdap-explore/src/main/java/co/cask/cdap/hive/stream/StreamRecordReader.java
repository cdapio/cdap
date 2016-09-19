/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data.stream.PositionStreamEvent;
import co.cask.cdap.data.stream.StreamDataFileReader;
import co.cask.cdap.data.stream.TimeRangeReadFilter;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@link org.apache.hadoop.mapred.RecordReader} for reading stream events in hive queries. This is different
 * enough from the mapreduce version that there is not a common class for the two.
 */
final class StreamRecordReader implements RecordReader<Void, ObjectWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamRecordReader.class);

  private final List<PositionStreamEvent> events;
  private final StreamDataFileReader reader;
  private final StreamInputSplit inputSplit;
  private final ReadFilter readFilter;
  private final Principal principal;
  private final StreamId streamId;
  private final AuthorizationEnforcer authorizationEnforcer;

  StreamRecordReader(InputSplit split, JobConf conf) throws IOException {
    ContextManager.Context context = ContextManager.getContext(conf);
    this.inputSplit = (StreamInputSplit) split;
    this.events = Lists.newArrayListWithCapacity(1);
    this.reader = createReader(FileSystem.get(conf), inputSplit);
    this.readFilter = new TimeRangeReadFilter(inputSplit.getStartTime(), inputSplit.getEndTime());
    Preconditions.checkArgument(context != null, "ContextManager should not be null in a Hive job.");
    Preconditions.checkArgument(context.getAuthenticationContext() != null,
                                "AuthenticationContext in Hive's StreamRecordReader should not be null.");
    this.principal = context.getAuthenticationContext().getPrincipal();
    this.authorizationEnforcer = context.getAuthorizationEnforcer();
    this.streamId = HiveStreamInputFormat.getStreamId(conf);
  }

  @Override
  public float getProgress() throws IOException {
    if (reader == null) {
      return 0.0f;
    }

    long processed = reader.getPosition() - inputSplit.getStart();
    return Math.min((float) processed / (float) inputSplit.getLength(), 1.0f);
  }

  @Override
  public boolean next(Void key, ObjectWritable value) throws IOException {
    // Make sure that the user has read access to the stream.
    try {
      authorizationEnforcer.enforce(streamId, principal, Action.READ);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(e);
    }

    events.clear();
    try {
      if (reader.read(events, 1, 0, TimeUnit.SECONDS, readFilter) <= 0) {
        return false;
      }
      PositionStreamEvent streamEvent = events.get(0);
      if (streamEvent.getStart() - inputSplit.getStart() >= inputSplit.getLength()) {
        return false;
      }
      value.set(streamEvent);
      return true;
    } catch (InterruptedException e) {
      LOG.error("interrupted while reading stream events.", e);
      return false;
    }
  }

  @Override
  public Void createKey() {
    return null;
  }

  @Override
  public ObjectWritable createValue() {
    // this method creates the value that is then passed into the next method, where it is set.
    return new ObjectWritable();
  }

  @Override
  public long getPos() throws IOException {
    // as far as I can tell, this doesn't do anything useful...
    return reader.getPosition();
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  /**
   * Creates a {@link co.cask.cdap.data.stream.StreamDataFileReader} based on the input split.
   *
   * @param fs The {@link org.apache.hadoop.fs.FileSystem} for the input.
   * @param inputSplit Split information.
   * @return A stream data file reader that is ready for reading events as specified by the input split.
   */
  private StreamDataFileReader createReader(FileSystem fs, StreamInputSplit inputSplit) throws IOException {
    StreamDataFileReader reader = StreamDataFileReader.createWithOffset(
      Locations.newInputSupplier(fs, inputSplit.getEventPath()),
      inputSplit.getIndexPath() == null ? null : Locations.newInputSupplier(fs, inputSplit.getIndexPath()),
      inputSplit.getStart());
    try {
      reader.initialize();
      return reader;
    } catch (IOException e) {
      try {
        reader.close();
      } catch (IOException closeEx) {
        e.addSuppressed(closeEx);
      }
      throw e;
    }
  }
}

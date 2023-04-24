/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.spark.streaming;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import scala.Option;

/**
 * Mock InputDStream that implements StreamingEventHandler.
 */
public class MockStreamingEventHandlerDStream extends InputDStream<StructuredRecord>
    implements StreamingEventHandler {

  private final org.apache.spark.streaming.StreamingContext ssc;
  private final List<String> recordsAsStrings;
  private final Schema schema;
  private final StreamingEventHandler delegate;
  private List<StructuredRecord> inputRecords;

  private AtomicBoolean firstRun = new AtomicBoolean();

  public MockStreamingEventHandlerDStream(org.apache.spark.streaming.StreamingContext ssc,
      List<String> recordsAsStrings, Schema schema, StreamingEventHandler delegate) {
    super(ssc, scala.reflect.ClassTag$.MODULE$.apply(String.class));
    this.ssc = ssc;
    this.recordsAsStrings = recordsAsStrings;
    this.schema = schema;
    this.delegate = delegate;
  }

  @Override
  public void start() {
    firstRun.set(true);
    inputRecords = new ArrayList<>();
    for (String recordStr : recordsAsStrings) {
      try {
        inputRecords.add(StructuredRecordStringConverter.fromJsonString(recordStr, schema));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void stop() {

  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
    if(!firstRun.get()) {
      return Option.empty();
    }

    firstRun.set(false);
    return Option.apply(new MockStreamingSourceRDD(ssc.sparkContext(), inputRecords));
  }

  @Override
  public void onBatchCompleted(StreamingContext streamingContext) {
    delegate.onBatchCompleted(streamingContext);
  }

  @Override
  public void onBatchStarted(StreamingContext streamingContext) {
    delegate.onBatchStarted(streamingContext);
  }

  @Override
  public void onBatchRetry(StreamingContext streamingContext) {
    delegate.onBatchRetry(streamingContext);
  }
}

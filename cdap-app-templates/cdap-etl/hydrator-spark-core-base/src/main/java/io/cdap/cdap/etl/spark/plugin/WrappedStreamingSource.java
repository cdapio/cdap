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

package co.cask.cdap.etl.spark.plugin;

import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.common.plugin.Caller;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.concurrent.Callable;

/**
 * Wrapper around a {@link Windower} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <T> type of object contained in the stream
 */
public class WrappedStreamingSource<T> extends StreamingSource<T> {
  private final StreamingSource<T> source;
  private final Caller caller;

  public WrappedStreamingSource(StreamingSource<T> source, Caller caller) {
    this.source = source;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        source.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public JavaDStream<T> getStream(final StreamingContext context) throws Exception {
    return caller.call(new Callable<JavaDStream<T>>() {
      @Override
      public JavaDStream<T> call() throws Exception {
        return source.getStream(context);
      }
    });
  }
}

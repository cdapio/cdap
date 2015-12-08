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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.cdap.etl.log.LogContext;

import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Wrapper around a {@link RealtimeSource} that makes sure logging is set correctly.
 *
 * @param <T> type of output
 */
public class LoggedRealtimeSource<T> extends RealtimeSource<T> {
  private final String name;
  private final RealtimeSource<T> realtimeSource;

  public LoggedRealtimeSource(String name, RealtimeSource<T> realtimeSource) {
    this.name = name;
    this.realtimeSource = realtimeSource;
  }

  @Nullable
  @Override
  public SourceState poll(final Emitter<T> writer, final SourceState currentState) throws Exception {
    return LogContext.run(new Callable<SourceState>() {
      @Override
      public SourceState call() throws Exception {
        return realtimeSource.poll(writer, currentState);
      }
    }, name);
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        realtimeSource.configurePipeline(pipelineConfigurer);
        return null;
      }
    }, name);
  }

  @Override
  public void initialize(final RealtimeContext context) throws Exception {
    LogContext.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        realtimeSource.initialize(context);
        return null;
      }
    }, name);
  }

  @Override
  public void destroy() {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        realtimeSource.destroy();
        return null;
      }
    }, name);
  }
}

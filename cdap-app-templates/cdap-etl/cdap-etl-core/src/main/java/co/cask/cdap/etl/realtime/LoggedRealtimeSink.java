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

import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.StageLoggingCaller;

import java.util.concurrent.Callable;

/**
 * Wrapper around {@link RealtimeSink} that makes sure logging is set up correctly.
 *
 * @param <I> the input type
 */
public class LoggedRealtimeSink<I> extends RealtimeSink<I> {
  private final RealtimeSink<I> realtimeSink;
  private final Caller caller;

  public LoggedRealtimeSink(String name, RealtimeSink<I> realtimeSink) {
    this.realtimeSink = realtimeSink;
    this.caller = StageLoggingCaller.wrap(Caller.DEFAULT, name);
  }

  @Override
  public int write(final Iterable<I> objects, final DataWriter dataWriter) throws Exception {
    return caller.call(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return realtimeSink.write(objects, dataWriter);
      }
    });
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        realtimeSink.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public void initialize(final RealtimeContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        realtimeSink.initialize(context);
        return null;
      }
    });
  }

  @Override
  public void destroy() {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        realtimeSink.destroy();
        return null;
      }
    });
  }
}

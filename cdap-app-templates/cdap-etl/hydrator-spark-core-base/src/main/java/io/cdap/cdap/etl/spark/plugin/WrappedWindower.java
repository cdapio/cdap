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
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.common.plugin.Caller;

import java.util.concurrent.Callable;

/**
 * Wrapper around a {@link Windower} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 */
public class WrappedWindower extends Windower {
  private final Windower windower;
  private final Caller caller;

  public WrappedWindower(Windower windower, Caller caller) {
    this.windower = windower;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        windower.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public long getWidth() {
    return caller.callUnchecked(new Callable<Long>() {
      @Override
      public Long call() {
        return windower.getWidth();
      }
    });
  }

  @Override
  public long getSlideInterval() {
    return caller.callUnchecked(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return windower.getSlideInterval();
      }
    });
  }
}

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

package co.cask.cdap.templates.etl.api.batch;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.StageLifecycle;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Batch Sink forms the last stage of a Batch ETL Pipeline.
 * @param <I> Object sink operates on
 */
public abstract class AbstractBatchSink<I> implements StageLifecycle {

  private MapReduceContext context;

  /**
   * Configure the Sink.
   * @param configurer {@link StageConfigurer}
   */
  public void configure(StageConfigurer configurer) {
    configurer.setName(this.getClass().getSimpleName());
  }

  /**
   * Prepare the Batch Job
   * @param context {@link MapReduceContext}
   */
  public abstract void prepareJob(MapReduceContext context);

  /**
   * Initialize the Sink.
   * @param context {@link MapReduceContext}
   */
  public void initialize(MapReduceContext context) {
    this.context = context;
  }

  /**
   * Write the given object
   * @param context {@link Mapper.Context}
   * @param object object to be written
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void write(Mapper.Context context, I object) throws IOException, InterruptedException;

  @Override
  public void destroy() {
    // no-op
  }

  /**
   * Invoked after Batch Job is completed
   * @param succeeded true if batch job completed successfully
   * @param context {@link MapReduceContext}
   */
  public abstract void onFinish(boolean succeeded, MapReduceContext context);

  protected MapReduceContext getContext() {
    return context;
  }
}

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

import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.StageLifecycle;

import java.io.IOException;

/**
 * Batch Sink forms the last stage of a Batch ETL Pipeline.
 *
 * @param <I> Object sink operates on
 * @param <KEY> Batch Output Key class
 * @param <VALUE> Batch Output Value class
 */
public abstract class AbstractBatchSink<I, KEY, VALUE> implements StageLifecycle {

  private BatchContext context;

  /**
   * Configure the Sink.
   *
   * @param configurer {@link StageConfigurer}
   */
  public void configure(StageConfigurer configurer) {
    configurer.setName(this.getClass().getSimpleName());
  }

  /**
   * Prepare the Batch Job. Used to configure the Hadoop Job before starting the Batch Job.
   *
   * @param context {@link BatchContext}
   */
  public abstract void prepareJob(BatchContext context);

  /**
   * Initialize the Sink. Invoked during at the start of the Batch Job.
   *
   * @param context {@link BatchContext}
   */
  public void initialize(BatchContext context) {
    this.context = context;
  }

  /**
   * Write the given object.
   *
   * @param object object to be written
   * @param writer Writer to persist data to Batch Output
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void write(I object, BatchWriter<KEY, VALUE> writer) throws IOException, InterruptedException;

  @Override
  public void destroy() {
    // no-op
  }

  /**
   * Invoked after Batch Job is completed.
   *
   * @param succeeded true if batch job completed successfully
   * @param context {@link BatchContext}
   */
  public abstract void onFinish(boolean succeeded, BatchContext context);

  protected BatchContext getContext() {
    return context;
  }
}

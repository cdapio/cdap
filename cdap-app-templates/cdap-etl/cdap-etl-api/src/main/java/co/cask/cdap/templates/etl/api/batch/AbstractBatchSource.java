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

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.StageLifecycle;

/**
 * Batch Source forms the first stage of a Batch ETL Pipeline.
 * @param <KEY> Batch Input Key class
 * @param <VALUE> Batch Input Value class
 * @param <O> Object that BatchSource emits
 */
public abstract class AbstractBatchSource<KEY, VALUE, O> implements StageLifecycle {

  private BatchContext context;

  /**
   * Configure the Batch Source stage.
   * @param configurer {@link StageConfigurer}
   */
  public void configure(StageConfigurer configurer) {
    configurer.setName(this.getClass().getSimpleName());
  }

  /**
   * Setup configuration related to the Batch Source.
   * @param context {@link BatchContext}
   */
  public abstract void prepareJob(BatchContext context);

  /**
   * Initialize the Batch Source.
   * @param context {@link BatchContext}
   */
  public void initialize(BatchContext context) {
    this.context = context;
  }

  /**
   * Process data.
   * @param key Key class from Input
   * @param value Value class from Input
   * @param data Emit data
   */
  public abstract void combine(KEY key, VALUE value, Emitter<O> data);

  @Override
  public void destroy() {
    // no-op
  }

  /**
   * Operation to be performed at the end of the Batch job.
   * @param succeeded true if Batch operation succeeded, false otherwise
   * @param context {@link BatchContext}
   * @throws Exception
   */
  public abstract void onFinish(boolean succeeded, BatchContext context) throws Exception;

  protected BatchContext getContext() {
    return context;
  }
}

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

package co.cask.cdap.etl.batch;

import co.cask.cdap.etl.api.Destroyable;
import com.google.common.base.Throwables;


/**
 * A stage in the PipeTransformExecutor. Pipe transforms should send each output record immediately to all output stages
 * without buffering anything in memory.
 *
 * @param <T> type of input record
 */
public abstract class PipeStage<T> implements Destroyable {
  private final String stageName;

  protected PipeStage(String stageName) {
    this.stageName = stageName;
  }

  /**
   * Consume a record and send it to the relevant output transforms.
   *
   * @param input the record to consume
   * @throws StageFailureException if there was an exception consuming the input
   */
  public void consume(T input) {
    try {
      consumeInput(input);
    } catch (StageFailureException e) {
      // Another stage has already failed, just throw the exception as-is
      throw e;
    } catch (Exception e) {
      Throwable rootCause = Throwables.getRootCause(e);
      // Create StageFailureException to save the Stage information
      throw new StageFailureException(
        String.format("Failed to execute pipeline stage '%s' with the error: %s. Please review your pipeline " +
                        "configuration and check the system logs for more details.", stageName, rootCause.getMessage()),
        rootCause);
    }
  }

  /**
   * Consume an input record, throwing an exception if there is a failure
   *
   * @param input the input to consume
   * @throws Exception if there was a failure processing the input
   */
  protected abstract void consumeInput(T input) throws Exception;
}

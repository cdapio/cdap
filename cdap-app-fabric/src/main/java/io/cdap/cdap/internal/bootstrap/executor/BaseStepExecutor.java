/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap.executor;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.proto.bootstrap.BootstrapStepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

/**
 * Executes a bootstrap step.
 *
 * @param <T> type of arguments required by the bootstrap step
 */
public abstract class BaseStepExecutor<T extends Validatable> implements BootstrapStepExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BaseStepExecutor.class);
  private static final Gson GSON = new Gson();

  @Override
  public BootstrapStepResult execute(String label, JsonObject argumentsObj) throws InterruptedException {
    T arguments;
    try {
      arguments = GSON.fromJson(argumentsObj, getArgumentsType());
    } catch (JsonParseException e) {
      LOG.warn("Bootstrap step {} failed because its arguments are malformed: {}", label, e.getMessage());
      return new BootstrapStepResult(label, BootstrapStepResult.Status.FAILED,
                                     String.format("Argument decoding failed. Reason: %s", e.getMessage()));
    }

    try {
      arguments.validate();
    } catch (RuntimeException e) {
      LOG.warn("Bootstrap step {} failed due to invalid arguments: {}", label, e.getMessage());
      return new BootstrapStepResult(label, BootstrapStepResult.Status.FAILED, e.getMessage());
    }

    try {
      LOG.debug("Executing bootstrap step {}", label);
      Retries.runWithInterruptibleRetries(() -> execute(arguments), getRetryStrategy(),
                                          t -> t instanceof RetryableException);
      LOG.debug("Bootstrap step {} completed successfully", label);
      return new BootstrapStepResult(label, BootstrapStepResult.Status.SUCCEEDED);
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Bootstrap step {} failed to execute", label, e);
      return new BootstrapStepResult(label, BootstrapStepResult.Status.FAILED, e.getMessage());
    }
  }

  /**
   * @return the class for the parameter of this class. For example, if T is EmptyArguments, this will return the class
   *         for EmptyArguments.
   */
  private Class<T> getArgumentsType() {
    Type superclass = getClass().getGenericSuperclass();
    //noinspection unchecked
    return (Class<T>) ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  /**
   * Execute the bootstrap step given the specified arguments
   *
   * @param arguments arguments required to execute the bootstrap step
   * @throws RetryableException if execution failed, but may succeed on retry
   * @throws Exception if execution failed in a way that can't be retried
   */
  protected abstract void execute(T arguments) throws Exception;

  /**
   * @return the retry strategy to use when executing this bootstrap step
   */
  protected RetryStrategy getRetryStrategy() {
    // exponential retries with a time limit of 5 minutes.
    return RetryStrategies.timeLimit(5, TimeUnit.MINUTES,
                                     RetryStrategies.exponentialDelay(200, 10000, TimeUnit.MILLISECONDS));
  }
}

/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.app.queue.InputDatum;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents a {@link ProcessMethod} that invocation is done through reflection.
 * @param <T> Type of input accepted by this process method.
 */
@NotThreadSafe
public final class ReflectionProcessMethod<T> implements ProcessMethod<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ReflectionProcessMethod.class);

  private final Flowlet flowlet;
  private final Method method;
  private final boolean hasParam;
  private final boolean batch;
  private final boolean needsIterator;
  private final boolean needContext;
  private final int maxRetries;

  public static <T> ReflectionProcessMethod<T> create(Flowlet flowlet, Method method, int maxRetries) {
    return new ReflectionProcessMethod<>(flowlet, method, maxRetries);
  }

  private ReflectionProcessMethod(Flowlet flowlet, Method method, int maxRetries) {
    this.flowlet = flowlet;
    this.method = method;
    this.maxRetries = maxRetries;

    this.hasParam = method.getGenericParameterTypes().length > 0;
    this.batch = method.isAnnotationPresent(Batch.class);
    this.needsIterator = hasParam &&
      TypeToken.of(method.getGenericParameterTypes()[0]).getRawType().equals(Iterator.class);
    this.needContext = method.getGenericParameterTypes().length == 2;

    if (!this.method.isAccessible()) {
      this.method.setAccessible(true);
    }
  }

  @Override
  public boolean needsInput() {
    return hasParam;
  }

  @Override
  public int getMaxRetries() {
    return maxRetries;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ProcessResult<T> invoke(InputDatum<T> input) {
    try {
      Preconditions.checkState(!hasParam || input.needProcess(), "Empty input provided to method that needs input.");
      InputContext inputContext = input.getInputContext();

      if (hasParam) {
        if (needsIterator) {
          invoke(method, input.iterator(), inputContext);
        } else {
          for (T event : input) {
            invoke(method, event, inputContext);
          }
        }
      } else {
        method.invoke(flowlet);
      }

      return createResult(input, null);
    } catch (Throwable t) {
      return createResult(input, t.getCause());
    }
  }

  @Override
  public String toString() {
    return flowlet.getClass() + "." + method.toString();
  }

  /**
   * Calls the user process method.
   */
  private void invoke(Method method, Object event, InputContext inputContext) throws Exception {
    if (needContext) {
      method.invoke(flowlet, event, inputContext);
    } else {
      method.invoke(flowlet, event);
    }
  }

  @SuppressWarnings("unchecked")
  private ProcessResult<T> createResult(InputDatum<T> input, Throwable failureCause) {
    // If the method has param, then object for the result would be iterator or the first event (batch vs no-batch)
    T event = hasParam ? (batch ? (T) input.iterator() : input.iterator().next()) : null;
    return new ReflectionProcessResult<>(event, failureCause);
  }

  private static final class ReflectionProcessResult<V> implements ProcessResult<V> {

    private final V event;
    private final Throwable cause;

    private ReflectionProcessResult(V event, Throwable cause) {
      this.event = event;
      this.cause = cause;
    }

    @Override
    public V getEvent() {
      return event;
    }

    @Override
    public boolean isSuccess() {
      return cause == null;
    }

    @Override
    public Throwable getCause() {
      return cause;
    }
  }
}

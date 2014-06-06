/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.annotation.Batch;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
    return new ReflectionProcessMethod<T>(flowlet, method, maxRetries);
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

      try {
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
    } catch (Exception e) {
      // System error if we reached here. E.g. failed to dequeue/decode event
      LOG.error("Fail to process input: {}", method, e);
      throw Throwables.propagate(e);
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
    return new ReflectionProcessResult<T>(event, failureCause);
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

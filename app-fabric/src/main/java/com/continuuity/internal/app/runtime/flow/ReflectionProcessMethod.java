/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
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
  private final boolean needsBatch;
  private final boolean needContext;
  private final int maxRetries;

  public static ReflectionProcessMethod create(Flowlet flowlet, Method method, int maxRetries) {
    return new ReflectionProcessMethod(flowlet, method, maxRetries);
  }

  private ReflectionProcessMethod(Flowlet flowlet, Method method, int maxRetries) {
    this.flowlet = flowlet;
    this.method = method;
    this.maxRetries = maxRetries;

    this.hasParam = method.getGenericParameterTypes().length > 0;
    this.needsBatch = hasParam &&
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

  @Override
  public ProcessResult invoke(InputDatum input, Function<ByteBuffer, T> inputDecoder) {
    try {
      Preconditions.checkState(!hasParam || input.needProcess(), "Empty input provided to method that needs input.");

      T event = null;
      if (hasParam) {
        Iterator<T> dataIterator = Iterators.transform(input.iterator(), inputDecoder);

        if (needsBatch) {
          //noinspection unchecked
          event = (T) dataIterator;
        } else {
          event = dataIterator.next();
        }
      }
      InputContext inputContext = input.getInputContext();

      try {
        if (hasParam) {
          if (needContext) {
            method.invoke(flowlet, event, inputContext);
          } else {
            method.invoke(flowlet, event);
          }
        } else {
          method.invoke(flowlet);
        }

        return new ReflectionProcessResult<T>(event, true, null);
      } catch (Throwable t) {
        return new ReflectionProcessResult<T>(event, false, t.getCause());
      }
    } catch (Exception e) {
      // If it reaches here, something very wrong.
      LOG.error("Fail to process input: {}", method, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String toString() {
    return flowlet.getClass() + "." + method.toString();
  }

  private static final class ReflectionProcessResult<V> implements ProcessResult<V> {

    private final V event;
    private final boolean success;
    private final Throwable cause;

    private ReflectionProcessResult(V event, boolean success, Throwable cause) {
      this.event = event;
      this.success = success;
      this.cause = cause;
    }

    @Override
    public V getEvent() {
      return event;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public Throwable getCause() {
      return cause;
    }
  }
}

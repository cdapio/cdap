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

package co.cask.cdap.internal.app.queue;

import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.app.queue.InputDatum;
import co.cask.cdap.common.queue.QueueName;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link co.cask.cdap.app.queue.InputDatum} that has nothing inside and is not from queue.
 *
 * @param <T> Type of input.
 */
public final class NullInputDatum<T> implements InputDatum<T> {

  private final AtomicInteger retries = new AtomicInteger(0);
  private final InputContext inputContext =  new InputContext() {
    @Override
    public String getOrigin() {
      return "";
    }

    @Override
    public int getRetryCount() {
      return retries.get();
    }

    @Override
    public String toString() {
      return "nullInput";
    }
  };

  @Override
  public boolean needProcess() {
    return true;
  }

  @Override
  public void incrementRetry() {
    retries.incrementAndGet();
  }

  @Override
  public int getRetry() {
    return retries.get();
  }

  @Override
  public InputContext getInputContext() {
    return inputContext;
  }

  @Override
  public QueueName getQueueName() {
    return null;
  }

  @Override
  public void reclaim() {
    // No-op
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("retries", retries.get())
      .toString();
  }
}

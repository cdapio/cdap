/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.app.queue;

import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.common.queue.QueueName;

/**
 * Represents a dequeue result from {@link QueueReader}.
 *
 * @param <T> Type of input.
 */
public interface InputDatum<T> extends Iterable<T> {

  boolean needProcess();

  void incrementRetry();

  int getRetry();

  InputContext getInputContext();

  QueueName getQueueName();

  /**
   * Reclaim the input from the queue consumer. It is needed for processing retried entries.
   */
  void reclaim();

  /**
   * Returns number of entries in this Iterable.
   */
  int size();
}

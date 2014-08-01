/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.app.queue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This interface defines reading of a {@link InputDatum} from the Queue.
 *
 * @param <T> Type of input dequeued from this reader.
 */
public interface QueueReader<T> {

  /**
   * Reads an input from the queue.
   *
   * @param timeout Maximum time for trying to have a non-empty dequeue result.
   *                Depending on the implementation, actual time spent for dequeue
   *                could be longer than the time specified here.
   * @param timeoutUnit Unit for the timeout.
   *
   * @return A {@link InputDatum} which
   *         represents the input being read from the queue.
   * @throws IOException If fails to dequeue.
   * @throws InterruptedException If dequeue is interrupted.
   */
  InputDatum<T> dequeue(long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException;
}

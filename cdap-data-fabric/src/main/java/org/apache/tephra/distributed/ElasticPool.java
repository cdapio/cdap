/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An Elastic Pool is an object pool that can dynamically grow.
 * Normally, an element is obtained by a client and then returned to the pool
 * after use. However, if the element gets into a bad state, the element can
 * be discarded, based upon the recycle() method returning false. This will
 * cause the element to be removed from the pool, and for a subsequent request,
 * a new element can be created on the fly to replace the discarded one.
 *
 * The pool starts with zero (active) elements. Every time a client attempts
 * to obtain an element, an element from the pool is returned if available.
 * Otherwise, if the number of active elements is less than the pool's limit,
 * a new element is created (using abstract method create(), this must be
 * overridden by all implementations), and the number of active elements is
 * increased by one. If the limit is reached, then obtain() blocks until
 * either an element is returned to the pool or, if the obtain method with timeout
 * parameters is used, a timeout occurs.
 *
 * Every time an element is returned to the pool, it is "recycled" to restore its
 * fresh state for the next use or destroyed, depending on its state.
 *
 * @param <T> the type of the elements
 * @param <E> the type of exception thrown by create()
 */
public abstract class ElasticPool<T, E extends Exception> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ElasticPool.class);

  /**
   * A method to create a new element. Will be called every time the pool
   * of available elements is empty but the limit of active elements is
   * not exceeded.
   * @return a new element
   */
  protected abstract T create() throws E;

  /**
   * A method to recycle an existing element when it is returned to the pool.
   * This methods ensures that the element is in a fresh state before it can
   * be reused by the next agent. If the element is not to be returned to the pool,
   * calling code is responsible for destroying it and returning false.
   *
   * @param element the element to recycle
   * @return true to reuse element, false to remove it from the pool
   */
  protected boolean recycle(T element) {
    // by default, simply return true
    return true;
  }

  // holds all currently available elements
  private final ConcurrentLinkedQueue<T> elements;

  // we keep track of elements via the permits of a semaphore, because there can
  // be elements in a queue, but also elements that are "loaned out" count towards
  // the pool's size limit
  private final Semaphore semaphore;

  public ElasticPool(int sizeLimit) {
    elements = new ConcurrentLinkedQueue<>();
    semaphore = new Semaphore(sizeLimit, true);
  }

  /**
   * Get a element from the pool. If there is an available element in
   * the pool, it will be returned. Otherwise, if the number of active
   * elements does not exceed the limit, a new element is created with
   * create() and returned. Otherwise, blocks until an element is either
   * released and returned to the pool, or an element is discarded,
   * allowing for the creation of a new element.
   *
   * @return an element
   */
  public T obtain() throws E, InterruptedException {
    semaphore.acquire();
    return getOrCreate();
  }

  /**
   * Get a element from the pool. If there is an available element in
   * the pool, it will be returned. Otherwise, if the number of active
   * elements does ot exceed the limit, a new element is created with
   * create() and returned. Otherwise, blocks until an element is either
   * released and returned to the pool, an element is discarded,
   * allowing for the creation of a new element, or a timeout occurs.
   *
   * @param timeout the timeout for trying to obtain an element
   * @param unit the timeout unit for trying to obtain an element
   * @return an element
   * @throws TimeoutException if a client is not able to be obtained within the given timeout
   */
  public T obtain(long timeout, TimeUnit unit) throws E, TimeoutException, InterruptedException {
    if (!semaphore.tryAcquire(1, timeout, unit)) {
      throw new TimeoutException(String.format("Failed to obtain client within %d %s.",
                                               timeout, unit));
    }
    return getOrCreate();
  }

  // gets an element from the queue, or creates one if there is none available in the queue.
  // the semaphore must be acquired before calling this method. The semaphore will be released from within
  // this method if it throws any exception
  private T getOrCreate() throws E {
    try {
      T client = elements.poll();
      // a client was available, all good. otherwise, create one
      if (client != null) {
        return client;
      }
      return create();
    } catch (Exception e) {
      // if an exception is thrown after acquiring the semaphore, release the
      // semaphore before propagating the exception
      semaphore.release();
      throw e;
    }
  }

  /**
   * Returns an element to the pool of available elements. The element must
   * have been obtained from this pool through obtain(). The recycle() method
   * is called before the element is available for obtain().
   *
   * @param element the element to be returned
   */
  public void release(T element) {
    try {
      if (recycle(element)) {
        elements.add(element);
      }
    } finally {
      semaphore.release();
    }
  }
}

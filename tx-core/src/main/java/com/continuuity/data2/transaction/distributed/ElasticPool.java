package com.continuuity.data2.transaction.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * An Elastic Pool is an object pool that can dynamically shrink and grow.
 * Normally, an element is obtained by a client and then returned to the pool
 * after use. However, if the element gets into a bad state, the client can
 * also discard the element. This will cause the element to be removed from
 * the pool, and for a subsequent request, a new element can be created
 * on the fly to replace the discarded one.
 *
 * The pool starts with zero (active) elements. Every time a client attempts
 * to obtain an element, an element from the pool is returned if available.
 * Otherwise, if the number of active elements is less than the pool's limit,
 * a new element is created (using abstract method create(), this must be
 * overridden by all implementations), and the number of active elements is
 * increased by one. If the limit is reached, then obtain() blocks until
 * either an element is returned to the pool, or an element is discarded,
 * allowing for the creation of a new element.
 *
 * Every time an element is discarded, it is "destroyed" in order to properly
 * release all it resources before discarding. Every time an element is
 * returned to the pool, it is "recycled" to restore its fresh state for the
 * next use.
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
   * A method to destroy an element. This gets called every time an element
   * is discarded, to properly release all resources that the element might
   * hold.
   *
   * @param element the element to destroy
   */
  protected void destroy(T element) {
    // by default do nothing
  }

  /**
   * A method to recycle an existing element when it is returned to the pool.
   * This methods ensures that the element is in a fresh state before it can
   * be reused by the next agent.
   *
   * @param element the element to recycle
   */
  protected void recycle(T element) {
    // by default do nothing
  }

  // holds all currently available elements
  private BlockingQueue<T> elements;

  // number of current acvtive elements (including ones that are in use)
  int size = 0;

  // the limit for the number of active elements
  int limit;

  public ElasticPool(int sizeLimit) {
    elements = new ArrayBlockingQueue<T>(sizeLimit);
    limit = sizeLimit;
    size = 0;
  }

  /**
   * Get a element from the pool. If there is an available element in
   * the pool, it will be returned. Otherwise, if the number of active
   * elements does ot exceed the limit, a new element is created with
   * create() and returned. Otherwise, blocks until an element is either
   * released and returned to the pool, or an element is discarded,
   * allowing for the creation of a new element.
   *
   * @return an element
   */
  public T obtain() throws E {
    T element = getOrCreate();
    while (true) {
      if (element != null) {
        return element;
      }
      synchronized (this) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          LOG.warn("Wait interrupted. Don't know what to do. Ignoring.");
          continue;
        }
        // got notified - try to get element
        element = getOrCreate();
      }
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
    synchronized (this) {
      this.recycle(element);
      this.elements.add(element);
      this.notify();
    }
  }

  /**
   * Discard an element from the pool. The element must have been obtained
   * from this pool. The destroy() method will be called. This decreases the
   * number of active elements, allowing for subsequent creation of a new one.
   * @param element
   */
  public void discard(T element) {
    synchronized (this) {
      this.destroy(element);
      this.size--;
      this.notify();
    }
  }

  /**
   * If the pool has an available element, return it. Otherwise,
   * if the active element limit is not reached, create a new element and
   * return it. Otherwise return null.
   * @return An elememt or null if an element is neither available nor can
   * one be created.
   */
  private T getOrCreate() throws E {
    T client = elements.poll();
    if (client != null) {
      // a client was available, all good
      return client;
    }
    // no client available but we have not reached the max number of clients:
    // create a new client but synchronize to make sure to avoid race with
    // other threads.
    synchronized (this) {
      // verify that nobody has created a client in the meantime
      if (size >= limit) {
        // max clients was reached since we first checked -> block and wait
        return null;
      }
      client = this.create();
      this.size++;
      return client;
    }
  }
}

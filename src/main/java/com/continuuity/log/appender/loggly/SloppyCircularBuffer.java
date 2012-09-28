package com.continuuity.log.appender.loggly;


import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A thread safe circular buffer that blocks on
 * {@link SloppyCircularBuffer#dequeue()} if the buffer is empty. It's possible
 * that concurrent threads calling {@link SloppyCircularBuffer#enqueue(Object)}
 * will remove more elements than actually required. This is deemed as an
 * acceptable behavior of this implementation.
 *
 * @param <Item> The type of items this {@link SloppyCircularBuffer} contains
 *
 * NOTE: USELESS REMOVE THIS PIECE OF SHIT.
 */
public final class SloppyCircularBuffer<Item> {
  private final BlockingQueue<Item> queue;

  public SloppyCircularBuffer(final int capacity) {
    this.queue = new ArrayBlockingQueue<Item>(capacity);
  }

  /**
   * Add the given Item to this {@link SloppyCircularBuffer} removing older
   * entries if necessary.
   * @param item
   */
  public void enqueue(final Item item) {
    for(; !this.queue.offer(item); this.queue.poll(), Thread.yield());
  }

  /**
   * Remove and return the eldest entry on this {@link SloppyCircularBuffer}
   * blocking if no item is available.
   *
   * @return
   * @throws InterruptedException if interrupted while waiting for an item to
   * become available.
   */
  public Item dequeue() throws InterruptedException {
    return this.queue.take();
  }

  /**
   * Remove and return the eldest entry on this {@link SloppyCircularBuffer}
   * blocking for the given timeout if no item is available. Will return
   * <code>null</code> if no item was available and the given timeout elapsed.
   *
   * @param timeout
   * @param unit
   * @return
   * @throws InterruptedException
   */
  public Item dequeue(final long timeout, final TimeUnit unit)
    throws InterruptedException {
    return this.queue.poll(timeout, unit);
  }
}
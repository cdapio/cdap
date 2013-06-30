package com.continuuity.metrics2.common;

/**
 * ZipIterator interface that is invoked by passing ith elements
 * from two list that are being iterated upon.
 *
 * @param <T> Defines the first iterator
 * @param <U> Defines the second iterator.
 */
public interface ZipIterator<T, U> {

  /**
   * Specifies the iterator to be advanced.
   */
  public enum Advance {
    BOTH,   // Both iterators to be advanced.
    ITER_A, // Advance only iterator A
    ITER_B  // Advance only iterator B
  };

  /**
   * Invoked for ith element on the list.
   *
   * @param t Iterator 1 type object.
   * @param u Iterator 2 type object.
   * @return true if handled successfully.
   */
  boolean each(T t, U u, Advance advance);

  /**
   * Invoke after processing the ith element in the list
   * requesting what iterator need to be advanced.
   *
   * @param t Iterator 1 type object.
   * @param u Iterator 2 type object.
   * @return which iterator needs to be advanced.
   */
  Advance advance(T t, U u);
}



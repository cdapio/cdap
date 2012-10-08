package com.continuuity.metrics2.common;

/**
 * ZipIterator interface that is invoked by passing ith elements
 * from two list that are being iterated upon.
 */
public interface ZipIterator<T, U> {
  /**
   * Invoked for ith element on the list.
   * @param t Iterator 1 type object.
   * @param u Iterator 2 type object.
   * @return true if handled successfully.
   */
  boolean each(T t, U u);
}



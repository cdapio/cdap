package com.continuuity.internal.io;

/**
 * @param <T> type instantiator
 */
public interface Instantiator<T> {

  T create();
}

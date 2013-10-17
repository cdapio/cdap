package com.continuuity.common.lang;

/**
 * @param <T> type instantiator
 */
public interface Instantiator<T> {

  T create();
}

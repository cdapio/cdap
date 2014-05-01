/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

/**
 * Represents state of a consumer.
 *
 * @param <T> Type of the state information.
 */
// TODO: Unify with the HBaseConsumerState
public interface ConsumerState<T> {

  long getGroupId();

  int getInstanceId();

  T getState();

  void setState(T state);
}

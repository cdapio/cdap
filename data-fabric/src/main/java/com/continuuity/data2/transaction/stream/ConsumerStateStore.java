/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * Represents storage for {@link ConsumerState}.
 *
 * @param <S> Type of state object this store can operate on.
 * @param <T> Type of state information that the {@link ConsumerState} contains
 */
// TODO: Unify with HBaseConsumerStateStore
public interface ConsumerStateStore<S extends ConsumerState<T>, T> extends Closeable {

  void getAll(Collection<? super S> result) throws IOException;

  void getByGroup(long groupId, Collection<? super S> result) throws IOException;

  /**
   * @returns the consumer state for the given groupId and instanceId.
   *          If no state is found, a state object with empty state info will be returned.
   *          If no such consumer exists in the state store, {@code null} will be returned.
   */
  S get(long groupId, int instanceId) throws IOException;

  void save(S state) throws IOException;

  void save(Iterable<? extends S> states) throws IOException;

  void remove(Iterable<? extends S> states) throws IOException;
}

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

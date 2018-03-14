/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.registry.internal.pair;

import co.cask.cdap.common.id.Id;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.EntityId;

/**
 * Used to serialize/deserialize {@link Id}s into {@link MDSKey}.
 *
 * @param <T> type of Id.
 */
public interface KeyMaker<T extends EntityId> {
  /**
   * Serializes Id as MDSKey.
   *
   * @param id id
   * @return MDSKey for id
   */
  MDSKey getKey(T id);

  /**
   * Given a {@link MDSKey.Splitter}, deserialize Id from it.
   *
   * @param splitter splitter for id
   * @return id
   */
  T getElement(MDSKey.Splitter splitter);

  /**
   * Given {@link MDSKey.Splitter}, skip the fields of Id. This is used during de-serialization.
   *
   * @param splitter splitter for id
   */
  void skipKey(MDSKey.Splitter splitter);
}

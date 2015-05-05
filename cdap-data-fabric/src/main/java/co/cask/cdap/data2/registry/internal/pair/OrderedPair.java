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

import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Represents a mapping between two {@link Id}s - FIRST and SECOND using MDSKey.
 * Uses {@link KeyMaker} to serialize/deserialzie ids into MDSKey.
 *
 * @param <FIRST> type of first element
 * @param <SECOND> type of second element
 */
public class OrderedPair<FIRST extends Id, SECOND extends Id> {
  private final KeyMaker<FIRST> keyMaker1;
  private final KeyMaker<SECOND> keyMaker2;
  private final String prefix;

  public OrderedPair(KeyMaker<FIRST> keyMaker1, KeyMaker<SECOND> keyMaker2, String prefix) {
    this.keyMaker1 = keyMaker1;
    this.keyMaker2 = keyMaker2;
    this.prefix = prefix;
  }

  public String getPrefix() {
    return prefix;
  }

  public MDSKey makeKey(FIRST first, SECOND second) {
    return new MDSKey.Builder()
      .add(getPrefix())
      .append(keyMaker1.getKey(first))
      .append(keyMaker2.getKey(second))
      .build();
  }

  public MDSKey makeScanKey(FIRST first) {
    return new MDSKey.Builder()
      .add(getPrefix())
      .append(keyMaker1.getKey(first))
      .build();
  }

  public Set<SECOND> getSecond(Set<MDSKey> mdsKeys) {
    Set<SECOND> secondSet = Sets.newHashSetWithExpectedSize(mdsKeys.size());
    for (MDSKey mdsKey : mdsKeys) {
      MDSKey.Splitter splitter = mdsKey.split();
      splitter.skipString(); // prefix
      keyMaker1.skipKey(splitter);
      secondSet.add(this.keyMaker2.getElement(splitter));
    }
    return secondSet;
  }
}

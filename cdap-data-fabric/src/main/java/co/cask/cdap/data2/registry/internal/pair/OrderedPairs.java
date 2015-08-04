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

import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Registry of {@link KeyMaker}s for {@link Id}s.
 */
public class OrderedPairs {
  private final Map<String, KeyMaker<? extends Id>> keyMakers;

  public OrderedPairs(Map<String, KeyMaker<? extends Id>> keyMakers) {
    this.keyMakers = ImmutableMap.copyOf(keyMakers);
  }

  public <FIRST extends Id, SECOND extends Id>
  OrderedPair<FIRST, SECOND> get(String first, String second) {
    @SuppressWarnings("unchecked")
    KeyMaker<FIRST> keyMaker1 = (KeyMaker<FIRST>) keyMakers.get(first);
    @SuppressWarnings("unchecked")
    KeyMaker<SECOND> keyMaker2 = (KeyMaker<SECOND>) keyMakers.get(second);
    return new OrderedPair<>(keyMaker1, keyMaker2, first + second);
  }
}

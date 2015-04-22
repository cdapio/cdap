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

package co.cask.cdap.common.collect;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Map;

/**
 * This collector will collect until it runs out of memory, it
 * never returns false.
 *
 * @param <KEY> Type of key
 * @param <VALUE> Type of value
 */
public class AllPairCollector<KEY, VALUE> implements PairCollector<KEY, VALUE> {

  private final Multimap<KEY, VALUE> elements = HashMultimap.create();


  @Override
  public boolean addEntries(Iterable<Map.Entry<KEY, VALUE>> entries) {
    for (Map.Entry<KEY, VALUE> entry : entries) {
      elements.put(entry.getKey(), entry.getValue());
    }
    return true;
  }

  @Override
  public <T extends Multimap<? super KEY, ? super VALUE>> T finish(T map) {
    map.putAll(elements);
    elements.clear();
    return map;
  }
}

/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.NavigableMap;

/**
 * Map Transform Utility
 */
public class MapTransformUtil {

  public static <K, VA, VB> NavigableMap<K, NavigableMap<K, VB>> transformMapValues(NavigableMap<K,
    NavigableMap<K, VA>> map, Function<VA, VB> transformer, Comparator comparator) {
    NavigableMap<K, NavigableMap<K, VB>> transformedTable = Maps.newTreeMap(comparator);
    for (NavigableMap.Entry<K, NavigableMap<K, VA>> entry : map.entrySet()) {
      transformedTable.put(entry.getKey(), Maps.transformValues(entry.getValue(), transformer));
    }
    return transformedTable;
  }
}

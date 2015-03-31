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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.templates.etl.api.Emitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DefaultEmitter implements Emitter, Iterable<Map.Entry> {
  private final List<Map.Entry> entryList;

  public DefaultEmitter() {
    entryList = Lists.newArrayList();
  }

  @Override
  public void emit(Object key, Object value) {
    entryList.add(Maps.immutableEntry(key, value));
  }

  @Override
  public Iterator iterator() {
    return entryList.iterator();
  }

  public void reset() {
    entryList.clear();
  }
}

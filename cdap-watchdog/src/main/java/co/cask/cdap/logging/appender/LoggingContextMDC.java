/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.appender;

import co.cask.cdap.common.logging.LoggingContext;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A {@link Map} representing the MDC property map of a logging event by combining
 * system tags and the user MDC. The entries in the system tags takes precedence.
 */
class LoggingContextMDC extends AbstractMap<String, String> {

  private final Map<String, String> eventMDC;
  private volatile Map<String, String> systemTags;

  LoggingContextMDC(LoggingContext loggingContext, Map<String, String> eventMDC) {
    this.systemTags = loggingContext.getSystemTagsAsString();
    this.eventMDC = eventMDC;
  }

  /**
   * Puts a new key value pair to system tags.
   */
  synchronized void putSystemTag(String key, String value) {
    Map<String, String> newSystemTags = new HashMap<>(systemTags);
    newSystemTags.put(key, value);
    systemTags = newSystemTags;
  }

  /**
   * Puts a set of new key value pairs to system tags.
   */
  synchronized void putSystemTags(Map<String, String> tags) {
    Map<String, String> newSystemTags = new HashMap<>(systemTags);
    newSystemTags.putAll(tags);
    systemTags = newSystemTags;
  }

  @Override
  public boolean containsKey(Object key) {
    return systemTags.containsKey(key) || eventMDC.containsKey(key);
  }

  @Override
  public String get(Object key) {
    if (systemTags.containsKey(key)) {
      return systemTags.get(key);
    }
    return eventMDC.get(key);
  }

  @Override
  public String put(String key, String value) {
    if (systemTags.containsKey(key)) {
      throw new IllegalArgumentException("Not allow overwriting system tag " + key);
    }
    return eventMDC.put(key, value);
  }

  @Override
  public Set<Entry<String, String>> entrySet() {
    return new AbstractSet<Entry<String, String>>() {
      @Override
      public Iterator<Entry<String, String>> iterator() {
        return Iterators.concat(systemTags.entrySet().iterator(),
                                Iterators.filter(eventMDC.entrySet().iterator(),
                                                 new Predicate<Entry<String, String>>() {
                                                   @Override
                                                   public boolean apply(Entry<String, String> entry) {
                                                     return !systemTags.containsKey(entry.getKey());
                                                   }
                                                 }));
      }

      @Override
      public int size() {
        return Sets.union(systemTags.keySet(), eventMDC.keySet()).size();
      }
    };
  }
}

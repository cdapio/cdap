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

package com.continuuity.common.logging;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Provides handy base abstract implementation of the logging context that can be used by subclasses to simplify their
 * implementations.
 */
public abstract class AbstractLoggingContext implements LoggingContext {
  // Map looks not efficient here, it might be better to use set
  private Map<String, SystemTag> systemTags = Maps.newHashMap();

  /**
   * Sets system tag.
   * @param name tag name
   * @param value tag value
   */
  protected final void setSystemTag(String name, String value) {
    systemTags.put(name, new SystemTagImpl(name, value));
  }

  /**
   * Gets system tag value by tag name.
   * @param name tag name
   * @return system tag value
   */
  protected final String getSystemTag(String name) {
    return systemTags.get(name).getValue();
  }

  /**
   * @see com.continuuity.common.logging.LoggingContext#getSystemTags()
   */
  @Override
  public Collection<SystemTag> getSystemTags() {
    return Collections.unmodifiableCollection(systemTags.values());
  }

  /**
   * @see LoggingContext#getSystemTagsMap()
   */
  @Override
  public Map<String, SystemTag> getSystemTagsMap() {
    return Collections.unmodifiableMap(systemTags);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("systemTags", systemTags)
      .toString();
  }

  private static final class SystemTagImpl implements SystemTag {
    private final String name;
    private final String value;

    private SystemTagImpl(final String name, final String value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("name", name)
        .add("value", value)
        .toString();
    }
  }
}

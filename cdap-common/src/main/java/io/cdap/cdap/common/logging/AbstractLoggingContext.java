/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.common.logging;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Provides handy base abstract implementation of the logging context that can be used by subclasses to simplify their
 * implementations.
 */
public abstract class AbstractLoggingContext implements LoggingContext {

  public static final String TAG_YARN_APP_ID = ".yarnAppId";
  public static final String TAG_YARN_CONTAINER_ID = ".yarnContainerId";

  private static final Logger LOG = LoggerFactory.getLogger(AbstractLoggingContext.class);

  // Map looks not efficient here, it might be better to use set
  private final Map<String, SystemTag> systemTags;

  protected AbstractLoggingContext() {
    systemTags = Maps.newHashMap();

    // Try picking up the YARN container id from the env, parse it and set it to context
    String containerId = System.getenv("CONTAINER_ID");
    if (containerId == null) {
      return;
    }

    try {
      ContainerId yarnContainerId;
      try {
        // For Hadoop 2.6+, use ContainerId.fromString(String)
        Method fromString = ContainerId.class.getMethod("fromString", String.class);
        yarnContainerId = (ContainerId) fromString.invoke(null, containerId);
      } catch (NoSuchMethodException e) {
        // This is for older Hadoop
        yarnContainerId = ConverterUtils.toContainerId(containerId);
      }

      setSystemTag(TAG_YARN_APP_ID, yarnContainerId.getApplicationAttemptId().getApplicationId().toString());
      setSystemTag(TAG_YARN_CONTAINER_ID, yarnContainerId.toString());
    } catch (Exception e) {
      // Ignore any exception
      LOG.debug("Failed to set YARN application and container id to logging context", e);
    }
  }

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
   * @see io.cdap.cdap.common.logging.LoggingContext#getSystemTags()
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
  public Map<String, String> getSystemTagsAsString() {
    return Maps.transformValues(getSystemTagsMap(), SystemTag::getValue);
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

    @Override
    public String getName() {
      return name;
    }

    @Override
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

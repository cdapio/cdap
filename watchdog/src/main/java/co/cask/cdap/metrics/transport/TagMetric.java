/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.metrics.transport;

import com.google.common.base.Objects;

/**
 *
 */
public final class TagMetric {

  private final String tag;
  private final int value;

  public TagMetric(String tag, int value) {
    this.tag = tag;
    this.value = value;
  }

  public String getTag() {
    return tag;
  }

  public int getValue() {
    return value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(TagMetric.class)
      .add("tag", tag)
      .add("value", value)
      .toString();
  }
}

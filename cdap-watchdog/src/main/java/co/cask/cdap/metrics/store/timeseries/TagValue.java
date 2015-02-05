/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.metrics.store.timeseries;

import com.google.common.base.Objects;

import javax.annotation.Nullable;

/**
 * Represents tag and its value assigned to the {@link Fact}.
 */
public final class TagValue {
  private final String tagName;
  private final String value;

  public TagValue(String tagName, @Nullable String value) {
    this.tagName = tagName;
    this.value = value;
  }

  public String getTagName() {
    return tagName;
  }

  @Nullable
  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TagValue tagValue = (TagValue) o;

    return tagName.equals(tagValue.tagName) &&  Objects.equal(value, tagValue.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tagName, value);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("tagName", tagName).add("value", value).toString();
  }
}

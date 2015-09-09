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
package co.cask.cdap.proto;

import co.cask.cdap.api.data.format.FormatSpecification;

import java.util.Objects;

/**
 * Represents the configuration of a stream view, used when creating or updating a view.
 */
public class ViewSpecification {

  private final FormatSpecification format;

  public ViewSpecification(FormatSpecification format) {
    this.format = format;
  }

  public ViewSpecification(ViewSpecification existing) {
    this(existing.format);
  }

  public FormatSpecification getFormat() {
    return format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ViewSpecification that = (ViewSpecification) o;
    return Objects.equals(format, that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format);
  }
}

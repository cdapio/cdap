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

package co.cask.cdap.internal.specification;

import co.cask.cdap.internal.format.RecordFormat;
import co.cask.cdap.internal.io.Schema;
import com.google.common.base.Objects;

import java.util.Map;

/**
 * Specification for a {@link RecordFormat}, including the class, schema, and settings to use for the format.
 */
public class FormatSpecification {
  private final String formatClass;
  private final Schema schema;
  private final Map<String, String> settings;

  public FormatSpecification(String formatClass, Schema schema, Map<String, String> settings) {
    this.formatClass = formatClass;
    this.schema = schema;
    this.settings = settings;
  }

  public String getFormatClass() {
    return formatClass;
  }

  public Schema getSchema() {
    return schema;
  }

  public Map<String, String> getSettings() {
    return settings;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FormatSpecification)) {
      return false;
    }

    FormatSpecification that = (FormatSpecification) o;

    return Objects.equal(formatClass, that.formatClass) &&
      Objects.equal(schema, that.schema) &&
      Objects.equal(settings, that.settings);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(formatClass, schema, settings);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("formatClass", formatClass)
      .add("schema", schema)
      .add("settings", settings)
      .toString();
  }
}

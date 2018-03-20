/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.report;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import static co.cask.cdap.report.ReportField.FilterType.RANGE;
import static co.cask.cdap.report.ReportField.FilterType.VALUE;

/**
 * Represents the types of fields in a report.
 */
public enum ReportField {
  NAMESPACE("namespace", String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_SCOPE("artifact.scope", String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_NAME("artifact.name", String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_VERSION("artifact.version", String.class, Collections.singletonList(VALUE), false),
  APPLICATION_NAME("application.name", String.class, Collections.singletonList(VALUE), false),
  APPLICATION_VERSION("application.version", String.class, Collections.singletonList(VALUE), false),
  PROGRAM("program", String.class, Collections.singletonList(VALUE), false),
  RUN("run", String.class, Collections.singletonList(VALUE), false),
  STATUS("status", String.class, Collections.singletonList(VALUE), false),
  START("start", Long.class, Collections.singletonList(RANGE), true),
  END("end", Long.class, Collections.singletonList(RANGE), true),
  DURATION("duration", Long.class, Collections.singletonList(RANGE), true),
  USER("user", String.class, Collections.singletonList(VALUE), false),
  START_METHOD("startMethod", String.class, Collections.singletonList(VALUE), false),
  RUNTIME_ARGUMENTS("runtimeArguments", String.class, Collections.emptyList(), false),
  NUM_LOG_WARNINGS("numLogWarnings", Integer.class, Collections.singletonList(RANGE), true),
  NUM_LOG_ERRORS("numLogErrors", Integer.class, Collections.singletonList(RANGE), true),
  NUM_RECORDS_OUT("numRecordsOut", Integer.class, Collections.singletonList(RANGE), true);

  public final String name;
  private final Class valueClass;
  private final List<FilterType> applicableFilters;
  private final boolean sortable;

  private static final Map<String, ReportField> FIELD_NAME_MAP;

  static {
    FIELD_NAME_MAP = new HashMap<>();
    for (ReportField type : ReportField.values()) {
      FIELD_NAME_MAP.put(type.getName(), type);
    }
  }

  ReportField(String name, Class valueClass, List<FilterType> applicableFilters, boolean sortable) {
    this.name = name;
    this.valueClass = valueClass;
    this.applicableFilters = applicableFilters;
    this.sortable = sortable;
  }

  public String getName() {
    return name;
  }

  public Class getValueClass() {
    return valueClass;
  }

  public List<FilterType> getApplicableFilters() {
    return applicableFilters;
  }

  public boolean isSortable() {
    return sortable;
  }

  @Nullable
  public static ReportField valueOfFieldName(String fieldName) {
    return FIELD_NAME_MAP.get(fieldName);
  }

  public static boolean isValidField(String fieldName) {
    return FIELD_NAME_MAP.containsKey(fieldName);
  }

  /**
   * Type of the filter.
   */
  public enum FilterType {
    VALUE,
    RANGE
  }
}

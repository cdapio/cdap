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
package co.cask.cdap.report.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import static co.cask.cdap.report.util.ReportField.FilterType.RANGE;
import static co.cask.cdap.report.util.ReportField.FilterType.VALUE;

/**
 * Represents the types of fields in a report.
 */
public enum ReportField {
  NAMESPACE(Constants.NAMESPACE, String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_SCOPE(Constants.ARTIFACT_SCOPE, String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_NAME(Constants.ARTIFACT_NAME, String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_VERSION(Constants.ARTIFACT_VERSION, String.class, Collections.singletonList(VALUE), false),
  APPLICATION_NAME(Constants.APPLICATION_NAME, String.class, Collections.singletonList(VALUE), false),
  APPLICATION_VERSION(Constants.ARTIFACT_VERSION, String.class, Collections.singletonList(VALUE), false),
  PROGRAM(Constants.PROGRAM, String.class, Collections.singletonList(VALUE), false),
  RUN(Constants.RUN, String.class, Collections.singletonList(VALUE), false),
  STATUS(Constants.STATUS, String.class, Collections.singletonList(VALUE), false),
  START(Constants.START, Long.class, Collections.singletonList(RANGE), true),
  RUNNING(Constants.RUNNING, Long.class, Collections.singletonList(RANGE), true),
  END(Constants.END, Long.class, Collections.singletonList(RANGE), true),
  DURATION(Constants.DURATION, Long.class, Collections.singletonList(RANGE), true),
  USER(Constants.USER, String.class, Collections.singletonList(VALUE), false),
  START_METHOD(Constants.START_METHOD, String.class, Collections.singletonList(VALUE), false),
  RUNTIME_ARGUMENTS(Constants.RUNTIME_ARGUMENTS, String.class, Collections.emptyList(), false),
  NUM_LOG_WARNINGS(Constants.NUM_LOG_WARNINGS, Integer.class, Collections.singletonList(RANGE), true),
  NUM_LOG_ERRORS(Constants.NUM_LOG_ERRORS, Integer.class, Collections.singletonList(RANGE), true),
  NUM_RECORDS_OUT(Constants.NUM_RECORDS_OUT, Integer.class, Collections.singletonList(RANGE), true);

  private final String fieldName;
  private final Class valueClass;
  private final List<FilterType> applicableFilters;
  private final boolean sortable;

  private static final Map<String, ReportField> FIELD_NAME_MAP;

  static {
    FIELD_NAME_MAP = new HashMap<>();
    for (ReportField type : ReportField.values()) {
      FIELD_NAME_MAP.put(type.getFieldName(), type);
    }
  }

  ReportField(String fieldName, Class valueClass, List<FilterType> applicableFilters, boolean sortable) {
    this.fieldName = fieldName;
    this.valueClass = valueClass;
    this.applicableFilters = applicableFilters;
    this.sortable = sortable;
  }

  public String getFieldName() {
    return fieldName;
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

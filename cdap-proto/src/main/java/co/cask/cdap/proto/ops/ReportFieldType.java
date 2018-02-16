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

package co.cask.cdap.proto.ops;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static co.cask.cdap.proto.ops.ReportFieldType.FilterType.RANGE;
import static co.cask.cdap.proto.ops.ReportFieldType.FilterType.VALUE;

/**
 * Represents the types of fields in a report.
 */
public enum ReportFieldType {
  NAMSPACE("namespace", String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_SCOPE("artifact.scope", String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_NAME("artifact.name", String.class, Collections.singletonList(VALUE), false),
  ARTIFACT_VERSION("artifact.version", String.class, Collections.singletonList(VALUE), false),
  APPLICATION_NAME("application.name", String.class, Collections.singletonList(VALUE), false),
  APPLICATION_VERSION("application.version", String.class, Collections.singletonList(VALUE), false),
  PROGRAM("program", String.class, Collections.singletonList(VALUE), false),
  STATUS("status", String.class, Collections.singletonList(VALUE), false),
  START("start", Long.class, Collections.singletonList(RANGE), true),
  END("end", Long.class, Collections.singletonList(RANGE), true),
  DURATION("duration", Long.class, Collections.singletonList(RANGE), true),
  USER("user", String.class, Collections.singletonList(VALUE), false),
  START_METHOD("startMethod", String.class, Collections.singletonList(VALUE), false),
  RUNTIME_ARGUMENTS("runtimeArguments", String.class, Collections.emptyList(), false),
  MIN_MEMORY("minMemory", Integer.class, Collections.singletonList(RANGE), true),
  MAX_MEMORY("maxMemory", Integer.class, Collections.singletonList(RANGE), true),
  AVERAGE_MEMORY("averageMemory", Integer.class, Collections.singletonList(RANGE), true),
  MIN_NUM_CORES("minNumCores", Integer.class, Collections.singletonList(RANGE), true),
  MAX_NUM_CORES("maxNumCores", Integer.class, Collections.singletonList(RANGE), true),
  AVERAGE_NUM_CORES("averageNumCores", Integer.class, Collections.singletonList(RANGE), true),
  MIN_NUM_CONTAINERS("minNumContainers", Integer.class, Collections.singletonList(RANGE), true),
  MAX_NUM_CONTAINERS("maxNumContainers", Integer.class, Collections.singletonList(RANGE), true),
  AVERAGE_NUM_CONTAINERS("averageNumContainers", Integer.class, Collections.singletonList(RANGE), true),
  NUM_LOG_WARNINGS("numLogWarnings", Integer.class, Collections.singletonList(RANGE), true),
  NUM_LOG_ERRORS("numLogErrors", Integer.class, Collections.singletonList(RANGE), true),
  NUM_RECORDS_OUT("numRecordsOut", Integer.class, Collections.singletonList(RANGE), true);

  private final String fieldName;
  private final Class valueClass;
  private final List<FilterType> applicableFilters;
  private final boolean sortable;

  private static final Map<String, ReportFieldType> FIELD_NAME_MAP;

  static {
    FIELD_NAME_MAP = new HashMap<>();
    for (ReportFieldType type : ReportFieldType.values()) {
      FIELD_NAME_MAP.put(type.getFieldName(), type);
    }
  }

  ReportFieldType(String fieldName, Class valueClass, List<FilterType> applicableFilters, boolean sortable) {
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

  public static ReportFieldType valueOfFieldName(String fieldName) {
    return FIELD_NAME_MAP.get(fieldName);
  }

  /**
   * Type of the filter.
   */
  public enum FilterType {
    VALUE,
    RANGE
  }
}

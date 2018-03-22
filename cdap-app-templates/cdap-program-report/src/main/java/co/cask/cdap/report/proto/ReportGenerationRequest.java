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

package co.cask.cdap.report.proto;

import co.cask.cdap.report.util.ReportField;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Represents a request to generate a program run report in an HTTP request.
 */
public class ReportGenerationRequest {
  private final Long start;
  private final Long end;
  private final List<String> fields;
  @Nullable
  private final List<Sort> sort;
  @Nullable
  private final List<Filter> filters;

  public ReportGenerationRequest(Long start, Long end, List<String> fields, @Nullable List<Sort> sort,
                                 @Nullable List<Filter> filters) {
    this.start = start;
    this.end = end;
    this.fields = fields;
    this.sort = sort;
    this.filters = filters;
    this.validate();
  }

  /**
   * @return the start of the time range within which the report is generated. All program runs in the report must be
   *         still running at {@code start}, or with end time not earlier than {@code start}.
   */
  public Long getStart() {
    return start;
  }

  /**
   * @return the end of the time range within which the report is generated. All program runs in the report must
   *         start before {@code end}.
   */
  public Long getEnd() {
    return end;
  }

  /**
   * @return names of the fields to be included in the final report. Must be valid fields from {@link ReportField}.
   */
  public List<String> getFields() {
    return fields;
  }

  /**
   * @return the field to sort the report by. Currently only support a single field.
   */
  @Nullable
  public List<Sort> getSort() {
    return sort;
  }

  /**
   * @return the filters that must be satisfied for every record in the report.
   */
  @Nullable
  public List<Filter> getFilters() {
    return filters;
  }

  /**
   * Validates this {@link ReportGenerationRequest}
   *
   * @throws IllegalArgumentException if this request is not valid.
   */
  public void validate() {
    List<String> errors = new ArrayList<>();
    if (start == null) {
      errors.add("'start' must be specified.");
    }
    if (end == null) {
      errors.add("'end' must be specified.");
    }
    if (start >= end) {
      errors.add("'start' must be smaller than 'end'.");
    }
    if (fields == null || fields.isEmpty()) {
      errors.add("'fields' must be specified.");
    } else {
      errors.addAll(fields.stream().map(f -> new ReportGenerationRequest.Field(f).getError())
                      .filter(e -> e != null).collect(Collectors.toList()));
    }
    if (filters != null) {
      errors.addAll(filters.stream().map(Filter::getError).filter(e -> e != null).collect(Collectors.toList()));
    }
    if (sort != null) {
      if (sort.size() > 1) {
        errors.add("Currently only one field is supported in sort.");
      }
      errors.addAll(sort.stream().map(Sort::getError).filter(e -> e != null).collect(Collectors.toList()));
    }
    if (errors.size() > 0) {
      throw new IllegalArgumentException("Please fix the following errors in the report generation request: "
                                           + String.join("; ", errors));
    }
  }

  /**
   * Represents a flied in the report generation request.
   */
  public static class Field {
    private final String fieldName;

    public Field(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }

    /**
     * @return the error of this field that are not allowed in a valid report generation request, or {@code null} if
     *         no such error exists.
     */
    @Nullable
    public String getError() {
      if (ReportField.isValidField(fieldName)) {
        return null;
      }
      return String.format("Invalid field name '%s' in fields. Field name must be one of: [%s]",
                           fieldName, String.join(", ", ReportField.FIELD_NAME_MAP.keySet()));
    }
  }

}

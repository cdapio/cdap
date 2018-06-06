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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a filter that checks whether the value of a field is allowed to be included in the report.
 *
 * @param <T> type of the values
 */
public abstract class Filter<T> extends ReportGenerationRequest.Field {
  public Filter(String fieldName) {
    super(fieldName);
  }

  /**
   * Checks whether the given value of the field is allowed to be included in the report.
   *
   * @param value value of the field
   * @return {@code true} if the value is allowed, {@code false} otherwise.
   */
  public abstract boolean apply(T value);

  /**
   * Gets errors in the filter of a given filter type that are not allowed in a report generation request.
   *
   * @param filterType type of the filter
   * @return list of errors in the filter
   */
  protected List<String> getFilterTypeErrors(ReportField.FilterType filterType) {
    List<String> errors = new ArrayList<>();
    ReportField valueFilterField = ReportField.valueOfFieldName(getFieldName());
    if (valueFilterField != null && !valueFilterField.getApplicableFilters().contains(filterType)) {
      errors.add(String.format("Field '%s' cannot be filtered by %s. It can only be filtered by: [%s]",
                               getFieldName(), filterType.getPrettyName(),
                               valueFilterField.getApplicableFilters().stream()
                                 .map(ReportField.FilterType::getPrettyName).collect(Collectors.joining(","))));
    }
    return errors;
  }
}

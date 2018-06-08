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
import javax.annotation.Nullable;

/**
 * A class represents the field to sort the report by and the order of sorting by this field.
 */
public class Sort extends ReportGenerationRequest.Field {
  private final Order order;

  public Sort(String fieldName, Order order) {
    super(fieldName);
    this.order = order;
  }

  /**
   * @return the sorting order with this field
   */
  public Order getOrder() {
    return order;
  }

  @Override
  @Nullable
  public String getError() {
    List<String> errors = new ArrayList<>();
    String fieldError = super.getError();
    if (fieldError != null) {
      errors.add(fieldError);
    }
    ReportField sortField = ReportField.valueOfFieldName(getFieldName());
    if (sortField != null && !sortField.isSortable()) {
      errors.add(String.format("Field '%s' in sort is not sortable. Only fields: [%s] are sortable",
                               getFieldName(), String.join(", ", ReportField.SORTABLE_FIELDS)));
    }
    if (order == null) {
      errors.add("'order' cannot be null, it can only be ASCENDING or DESCENDING");
    }
    return errors.isEmpty() ? null :
      String.format("Sort %s contains these errors: %s", getFieldName(), String.join("; ", errors));
  }

  @Override
  public String toString() {
    return "Sort{" +
      "fieldName=" + getFieldName() +
      ", order=" + order +
      '}';
  }

  /**
   * The order to sort a field by.
   */
  public enum Order {
    ASCENDING,
    DESCENDING
  }
}

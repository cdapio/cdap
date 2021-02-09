/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.draft;

import java.util.Arrays;

/**
 * Class to hold information for a sorting request in a list API
 */
public class SortRequest {
  private final String fieldName;
  private final SortOrder order;

  public SortRequest(String fieldName, String sortOrder) {
    //Always change to lowercase since some databases dont support capital letters in column names
    this.fieldName = fieldName.toLowerCase();
    try {
      this.order = SortOrder.valueOf(sortOrder.toUpperCase());
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Sort order '%s' is not valid. Valid options are %s", sortOrder,
                                                       Arrays.toString(SortOrder.values())));
    }
  }

  public String getFieldName() {
    return fieldName;
  }

  public SortOrder getOrder() {
    return order;
  }

  enum SortOrder {
    ASC,
    DESC
  }
}

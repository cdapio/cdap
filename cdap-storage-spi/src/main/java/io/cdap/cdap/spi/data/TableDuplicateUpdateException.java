/*
 * Copyright © 2022 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.spi.data;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.spi.data.table.StructuredTableId;

/**
 * Thrown when a table duplicate update happens, like duplicate column or index.
 */
@Beta
public class TableDuplicateUpdateException extends RuntimeException {
  private final StructuredTableId id;

  public TableDuplicateUpdateException(StructuredTableId id) {
    super(String.format("System table '%s' duplicate update on columns or indexes.", id));
    this.id = id;
  }

  public StructuredTableId getId() {
    return id;
  }
}

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
 */

package io.cdap.cdap.storage.spanner;

import com.google.cloud.spanner.TransactionContext;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.StructuredTableInstantiationException;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;

/**
 * The Cloud Spanner context to get the table.
 */
public class SpannerStructuredTableContext implements StructuredTableContext {

  private final TransactionContext context;
  private final SpannerStructuredTableAdmin admin;

  /**
   * Constructor for SpannerStructuredTableContext.
   */
  public SpannerStructuredTableContext(TransactionContext context,
      SpannerStructuredTableAdmin admin) {
    this.context = context;
    this.admin = admin;
  }

  @Override
  public StructuredTable getTable(StructuredTableId tableId)
      throws StructuredTableInstantiationException, TableNotFoundException {
    return new SpannerStructuredTable(context, admin.getSpannerStructuredTableSchema(tableId),
        admin.getDatabaseClient());
  }

  @Override
  public String getStorageProvider() {
    return SpannerStorageProvider.NAME;
  }
}

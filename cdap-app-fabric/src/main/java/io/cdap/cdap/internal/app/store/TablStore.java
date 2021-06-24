/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import com.google.inject.Inject;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class TablStore {
  private static String out = "";
  private final TransactionRunner transactionRunner;

  @Inject
  public TablStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
    this.out = "{\n sample: val \n}";
  }

  /**
   * Add or update capability
   *
   * @param namespace
   * @param content
   * @throws IOException
   */
  public void addOrUpdateTabl(String namespace, String content) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tablTable = context.getTable(StoreDefinition.TablStore.TABL_STORE_TABLE);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TablStore.NAMESPACE_FIELD, namespace));
      fields.add(Fields.stringField(StoreDefinition.TablStore.CONTENT, content));
      fields.add(Fields.longField(StoreDefinition.TablStore.TIME_FIELD, System.currentTimeMillis()));
      tablTable.upsert(fields);
      this.out = fields.toString();
    }, IOException.class);
  }

  /**
   * returns the Json representation of the Table
   */
  public String jsonStr() throws IOException {
    return out;
  }

}

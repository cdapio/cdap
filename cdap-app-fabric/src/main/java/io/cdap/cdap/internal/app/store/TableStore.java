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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class TableStore {
  private static String out = "";
  private static ArrayList<String> contnt = new ArrayList<>();
  private final TransactionRunner transactionRunner;

  @Inject
  public TableStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
    this.out = "{\n use /edit to create the table! \n}";
  }

  /**
   * Add or update capability
   *
   * @param namespace
   * @param content
   * @throws IOException
   */
  public void addOrUpdateTabl(String namespace, String content, Boolean update) throws IOException {
    long curTime = System.currentTimeMillis();
    GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("US/East"));
    calendar.setTimeInMillis(curTime);
    DateTime jTime = new DateTime(curTime, DateTimeZone.forTimeZone(TimeZone.getTimeZone("US/East")));
    DateTimeFormatter forMat = DateTimeFormat.forPattern("HH:mm MM/dd/YYYY");
    String fTime = forMat.print(jTime);
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tablTable = context.getTable(StoreDefinition.TableStore.TABLE_STORE_TABLE);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TableStore.NAMESPACE_FIELD, namespace));
      fields.add(Fields.stringField(StoreDefinition.TableStore.CONTENT, content));
      fields.add(Fields.stringField(StoreDefinition.TableStore.TIME, fTime));
      tablTable.upsert(fields);

      String[] temp = content.split("\n     ");
      if (update && !content.equals("")) {
        for (String str : temp) {
          contnt.add(str);
        }
      }
      StringBuilder output = new StringBuilder();
      output.append("|           Table           |\n");
      output.append("|__________________________|\n");
      output.append("|   namespace: {           |\n      |" + namespace + "|\n| }");
      output.append("                          |\n");
      output.append("|   data: {                 |");
      if (update) {
        for (String s : contnt) {
          output.append("\n|     " + s + "       |");
        }
      } else {
        output.append("\n|     " + content + "        |");
      }
      output.append("\n| }                         |\n");
      output.append("|   time created: {           |\n|     " + fTime + "}      |\n");
      output.append("| }                       |");
      output.append("|__________________________|");
      this.out = output.toString();
    }, IOException.class);
  }

  /**
   * returns the Json representation of the Table
   */
  public String jsonStr() throws IOException {
    return out;
  }

  /**
   * Returns the lists of Contents in Table
   */
  public ArrayList<String> getContent() {
    return contnt;
  }
  public void remove(String s) throws IOException {
    contnt.remove(s);
    addOrUpdateTabl("default", "", true);
  }
}

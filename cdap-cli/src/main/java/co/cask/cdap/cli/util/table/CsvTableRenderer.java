/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.cli.util.table;

import com.google.common.base.Joiner;

import java.io.PrintStream;
import java.util.List;

/**
 * {@link TableRenderer} implementation to print a table in CSV format. E.g.
 *
 * pid,end status,start,stop
 * 9bd22850-0017-4a10-972a-bc5ca8173584,STOPPED,1405986408,0
 * 7f9f8054-a71f-48e3-965d-39e2aab16d5d,STOPPED,1405978322,0
 * e1a2d4a9-667c-40e0-86fa-32ea68cc25f6,STOPPED,1405645401,0
 * 9276574a-cc2f-458c-973b-aed9669fc80e,STOPPED,1405644974,0
 * 1c5868d6-04c7-443b-b4db-aab1c3368be3,STOPPED,1405457462,0
 * 4003fa1d-15bd-4a09-ad2b-f2c52b4dda54,STOPPED,1405456719,0
 * 531dff0a-0441-424b-ae5b-023cc7383344,STOPPED,1405454043,0
 * d9cae8f9-3fd3-45f4-b4e9-102ef38cf4e1,STOPPED,1405371545,0
 */
public class CsvTableRenderer implements TableRenderer {

  private static final Joiner CSV_JOINER = Joiner.on(",");

  @Override
  public void render(PrintStream output, Table table) {
    if (table.getHeader() != null) {
      output.println(CSV_JOINER.useForNull("").join(table.getHeader()));
    }

    for (List<String> row : table.getRows()) {
      String string = CSV_JOINER.useForNull("").join(row);
      output.println(string);
    }
  }
}

/*
 * Copyright 2012-2014 Cask, Inc.
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

package co.cask.cdap.shell.util;

import com.google.common.base.Joiner;

import java.io.PrintStream;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Utility class to print an ASCII table. e.g.
 *
 * +-----------------------------------------------------------------------+
 * | pid                                  | end status | start      | stop |
 * +-----------------------------------------------------------------------+
 * | 9bd22850-0017-4a10-972a-bc5ca8173584 | STOPPED    | 1405986408 | 0    |
 * | 7f9f8054-a71f-48e3-965d-39e2aab16d5d | STOPPED    | 1405978322 | 0    |
 * | e1a2d4a9-667c-40e0-86fa-32ea68cc25f6 | STOPPED    | 1405645401 | 0    |
 * | 9276574a-cc2f-458c-973b-aed9669fc80e | STOPPED    | 1405644974 | 0    |
 * | 1c5868d6-04c7-443b-b4db-aab1c3368be3 | STOPPED    | 1405457462 | 0    |
 * | 4003fa1d-15bd-4a09-ad2b-f2c52b4dda54 | STOPPED    | 1405456719 | 0    |
 * | 531dff0a-0441-424b-ae5b-023cc7383344 | STOPPED    | 1405454043 | 0    |
 * | d9cae8f9-3fd3-45f4-b4e9-102ef38cf4e1 | STOPPED    | 1405371545 | 0    |
 * +-----------------------------------------------------------------------+
 *
 * @param <T> type of object that the rows represent
 */
public class AsciiTable<T> {

  @Nullable
  private final Object[] header;
  private final List<T> rows;
  private final RowMaker<T> rowMaker;

  /**
   * @param header strings representing the header of the table
   * @param rows list of objects that represent the rows
   * @param rowMaker makes Object arrays from a row object
   */
  public AsciiTable(@Nullable String[] header, List<T> rows, RowMaker<T> rowMaker) {
    this.header = header;
    this.rows = rows;
    this.rowMaker = rowMaker;
  }

  /**
   * Prints the ASCII table to the {@link PrintStream} output.
   *
   * @param output {@link PrintStream} to print to
   */
  public void print(PrintStream output) {
    Object[][] contents = new Object[rows.size()][];
    for (int i = 0; i < rows.size(); i++) {
      contents[i] = rowMaker.makeRow(rows.get(i));
    }

    int[] columnWidths = calculateColumnWidths(header, contents);
    String[] fillers = new String[columnWidths.length];
    for (int i = 0; i < columnWidths.length; i++) {
      fillers[i] = "%-" + columnWidths[i] + "s";
    }

    String template = "| " + Joiner.on(" | ").join(fillers) + " |";

    if (header != null) {
      output.println(generateDivider(columnWidths));
      output.printf(template + "\n", header);
    }

    output.println(generateDivider(columnWidths));
    for (Object[] row : contents) {
      output.printf(template + "\n", row);
    }
    output.println(generateDivider(columnWidths));
  }

  private String generateDivider(int[] columnWidths) {
    StringBuilder sb = new StringBuilder();
    sb.append("+");
    for (int columnWidth : columnWidths) {
      sb.append(times("-", columnWidth + 2));
    }

    // one for each divider
    sb.append(times("-", columnWidths.length - 1));
    sb.append("+");
    return sb.toString();
  }

  private String times(String string, int times) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < times; i++) {
      sb.append(string);
    }
    return sb.toString();
  }

  private int[] calculateColumnWidths(Object[] header, Object[][] contents) {
    Object[] row = header != null ? header : contents[0];
    int[] columnWidths = new int[row.length];
    for (int i = 0; i < row.length; i++) {
      columnWidths[i] = Math.max(row[i].toString().length(), getMaxLength(contents, i));
    }
    return columnWidths;
  }

  private int getMaxLength(Object[][] rows, int column) {
    int maxLength = 0;
    for (Object[] row : rows) {
      if (row != null && row[column] != null) {
        String string = row[column].toString();
        if (string != null && string.length() > maxLength) {
          maxLength = string.length();
        }
      }
    }
    return maxLength;
  }
}

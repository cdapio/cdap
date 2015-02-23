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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * {@link TableRenderer} implementation to print an ASCII table in the alt style. e.g.
 *
 * +=======================================================================+
 * | pid                                  | end status | start      | stop |
 * +=======================================================================+
 * | 9bd22850-0017-4a10-972a-bc5ca8173584 | STOPPED    | 1405986408 | 0    |
 * | 7f9f8054-a71f-48e3-965d-39e2aab16d5d | STOPPED    | 1405978322 | 0    |
 * | e1a2d4a9-667c-40e0-86fa-32ea68cc25f6 | STOPPED    | 1405645401 | 0    |
 * | 9276574a-cc2f-458c-973b-aed9669fc80e | STOPPED    | 1405644974 | 0    |
 * | 1c5868d6-04c7-443b-b4db-aab1c3368be3 | STOPPED    | 1405457462 | 0    |
 * | 4003fa1d-15bd-4a09-ad2b-f2c52b4dda54 | STOPPED    | 1405456719 | 0    |
 * | 531dff0a-0441-424b-ae5b-023cc7383344 | STOPPED    | 1405454043 | 0    |
 * | d9cae8f9-3fd3-45f4-b4e9-102ef38cf4e1 | STOPPED    | 1405371545 | 0    |
 * +=======================================================================+
 *
 * E.g. when cells are multiple lines:
 *
 * +========================================================================================+
 * | c1                         | c2                         | c3333                        |
 * +========================================================================================+
 * | r1zzzzzzzzzzzzzzzzzzzzzzzz | r11                        | r1                           |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * | zzzzzzzzzzzzzzzzzzzzzzzzzz |                            |                              |
 * |----------------------------------------------------------------------------------------|
 * | r2                         | r2222 zzzzzzz z z z zzzzzz | r                            |
 * |                            | z z zzzzzzzzz zzzzzzz zzzz |                              |
 * |----------------------------------------------------------------------------------------|
 * | r3333                      | r3                         | r3                           |
 * |                            |                            | 1                            |
 * +========================================================================================+
 */
public class AltStyleTableRenderer implements TableRenderer {

  @Override
  public void render(PrintStream output, Table table) {
    List<String> header = table.getHeader();
    List<Row> rows = Lists.newArrayList();

    // Collects all output cells for all records.
    // If any record has multiple lines output, a row divider is printed between each row.
    boolean useRowDivider = false;
    for (String[] row : table.getRows()) {
      useRowDivider = generateRow(row, rows) || useRowDivider;
    }

    int[] columnWidths = calculateColumnWidths(table.getHeader(), rows);

    // If has header, prints the header.
    if (!header.isEmpty()) {
      outputDivider(output, columnWidths, '=', '+');
      for (int i = 0; i < columnWidths.length; i++) {
        output.printf("| %-" + columnWidths[i] + "s ", header.get(i));
      }
      output.printf("|").println();
    }

    // Prints a divider between header and first row if no divider is needed between rows.
    // Otherwise it's printed as part of the following row loop.
    char edgeChar = '+';
    char lineChar = '=';
    if (!useRowDivider) {
      outputDivider(output, columnWidths, lineChar, edgeChar);
    }

    // Output each row.
    for (Row row : rows) {
      if (useRowDivider) {
        // The first divider uses a different set of line and edge char
        // As it's either the separate for the header of the table border (without header case)
        outputDivider(output, columnWidths, lineChar, edgeChar);
        edgeChar = '|';
        lineChar = '-';
      }

      // Print each cell. It has to loop until all lines from all cells are printed.
      boolean done = false;
      int line = 0;
      while (!done) {
        done = true;
        for (int i = 0; i < row.size(); i++) {
          Cell cell = row.get(i);
          cell.output(output, "| %-" + columnWidths[i] + "s ", line);
          done = done && (line + 1 >= cell.size());
        }
        output.printf("|").println();
        line++;
      }
    }
    outputDivider(output, columnWidths, '=', '+');
  }

  /**
   * Prints a divider.
   *
   * @param output The {@link PrintStream} to output to
   * @param columnWidths Columns widths for each column
   * @param lineChar Character to use for printing the divider line
   * @param edgeChar Character to use for the left and right edge character
   */
  private void outputDivider(PrintStream output, int[] columnWidths, char lineChar, char edgeChar) {
    output.print(edgeChar);
    for (int columnWidth : columnWidths) {
      output.print(Strings.repeat(Character.toString(lineChar), columnWidth + 2));
    }

    // one for each divider
    output.print(Strings.repeat(Character.toString(lineChar), columnWidths.length - 1));
    output.print(edgeChar);
    output.println();
  }

  /**
   * Generates a record row. A record row can span across multiple lines on the screen.
   *
   * @param columns The set of columns to output.
   * @param collection Collection for collecting the generated {@link Row} object.
   * @return Returns true if the row spans multiple lines.
   */
  private boolean generateRow(String[] columns, Collection<? super Row> collection) {
    ImmutableList.Builder<Cell> builder = ImmutableList.builder();

    boolean multiLines = false;
    Splitter splitter = Splitter.on(System.getProperty("line.separator"));
    for (Object field : columns) {
      String fieldString = field == null ? "" : field.toString();
      Cell cell = new Cell(splitter.split(fieldString));
      multiLines = multiLines || cell.size() > 1;
      builder.add(cell);
    }

    collection.add(new Row(builder.build()));
    return multiLines;
  }

  /**
   * Calculates the maximum columns' widths.
   *
   * @param header The table header.
   * @param rows All rows that is going to display.
   * @return An array of integers, with contains maximum width for each column.
   */
  private int[] calculateColumnWidths(List<String> header, List<Row> rows) {
    int[] widths = new int[header.isEmpty() ? rows.get(0).size() : header.size()];

    for (int i = 0; i < header.size(); i++) {
      widths[i] = header.get(i).length();
    }

    // Find the maximum width for each column by consulting the width of each cell.
    for (Row row : rows) {
      for (int i = 0; i < row.size(); i++) {
        Cell cell = row.get(i);
        if (cell.getWidth() > widths[i]) {
          widths[i] = cell.getWidth();
        }
      }
    }
    return widths;
  }

  /**
   * Represents data in one output table cell, which the content can spans multiple lines.
   */
  private static final class Cell implements Iterable<String> {

    private final List<String> content;
    private final int width;

    Cell(Iterable<String> content) {
      this.content = ImmutableList.copyOf(content);
      int maxWidth = 0;
      for (String row : content) {
        if (row.length() > maxWidth) {
          maxWidth = row.length();
        }
      }
      this.width = maxWidth;
    }

    /**
     * Returns the maximum width of this cell content.
     */
    int getWidth() {
      return width;
    }

    /**
     * Writes a line to the given output with the given format.
     *
     * @param output The {@link PrintStream} to write to.
     * @param format The formatting string to use for printing.
     * @param line The line within this cell.
     */
    void output(PrintStream output, String format, int line) {
      output.printf(format, line >= content.size() ? "" : content.get(line));
    }

    /**
     * Returns the number of rows span for the content in this cell.
     */
    int size() {
      return content.size();
    }

    @Override
    public Iterator<String> iterator() {
      return content.iterator();
    }
  }


  /**
   * Represents a Row content in the output Table. Each row contains multiple cells.
   */
  private static final class Row implements Iterable<Cell> {

    private final List<Cell> cells;

    private Row(Iterable<Cell> cells) {
      this.cells = ImmutableList.copyOf(cells);
    }

    @Override
    public Iterator<Cell> iterator() {
      return null;
    }

    /**
     * Returns the {@link Cell} at the given column.
     */
    Cell get(int i) {
      return cells.get(i);
    }

    /**
     * Returns the number of cells this row contains.
     */
    int size() {
      return cells.size();
    }
  }
}

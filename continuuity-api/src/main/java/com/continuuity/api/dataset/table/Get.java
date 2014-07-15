/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.dataset.table;

import java.util.Collection;

/**
 * A Get reads one, multiple, or all columns of a row.
 */
public class Get extends RowColumns<Get> {
  /**
   * Get all of the columns of a row.
   * @param row Row to get.
   */
  public Get(byte[] row) {
    super(row);
  }

  /**
   * Get a set of columns of a row.
   * @param row Row to get.
   * @param columns Columns to get.
   */
  public Get(byte[] row, byte[]... columns) {
    super(row, columns);
  }

  /**
   * Get a set of columns of a row.
   * @param row Row to get.
   * @param columns Columns to get.
   */
  public Get(byte[] row, Collection<byte[]> columns) {
    super(row, columns.toArray(new byte[columns.size()][]));
  }

  /**
   * Get all of the columns of a row.
   * @param row Row to get.
   */
  public Get(String row) {
    super(row);
  }

  /**
   * Get a set of columns of a row.
   * @param row Row to get.
   * @param columns Columns to get.
   */
  public Get(String row, String... columns) {
    super(row, columns);
  }

  /**
   * Get a set of columns of a row.
   * @param row row to get
   * @param columns columns to get
   */
  public Get(String row, Collection<String> columns) {
    super(row, columns.toArray(new String[columns.size()]));
  }
}

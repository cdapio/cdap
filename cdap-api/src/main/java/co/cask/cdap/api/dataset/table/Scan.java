/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.table;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

/**
 * Scan configuration for {@link Table}.
 */
@Beta
public class Scan {
  @Nullable
  private final byte[] startRow;
  @Nullable
  private final byte[] stopRow;
  @Nullable
  private final Filter filter;

  /**
   * Creates {@link Scan} for a given start and stop row keys.
   * @param startRow start row inclusive; {@code null} means start from first row of the table
   * @param stopRow stop row exclusive; {@code null} means scan all rows to the end of the table
   */
  public Scan(@Nullable byte[] startRow, @Nullable byte[] stopRow) {
    this(startRow, stopRow, null);
  }

  /**
   * Creates {@link Scan} for a given start and stop row keys and filter.
   * @param startRow start row inclusive; {@code null} means start from first row of the table
   * @param stopRow stop row exclusive; {@code null} means scan all rows to the end of the table
   * @param filter filter to be used on scan
   */
  public Scan(@Nullable byte[] startRow, @Nullable byte[] stopRow, @Nullable Filter filter) {
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.filter = filter;
  }

  @Nullable
  public byte[] getStartRow() {
    return startRow;
  }

  @Nullable
  public byte[] getStopRow() {
    return stopRow;
  }

  @Nullable
  public Filter getFilter() {
    return filter;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("startRow", Bytes.toStringBinary(startRow))
      .add("stopRow", Bytes.toStringBinary(stopRow))
      .add("filter", filter)
      .toString();
  }
}

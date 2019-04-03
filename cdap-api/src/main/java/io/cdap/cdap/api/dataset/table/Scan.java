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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

  private final Map<String, String> properties = new HashMap<>();

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

  /**
   * Set a property for the Scan. Properties may be used to optimize performance,
   * and may not apply in all environments.
   * 
   * @param property the name of the property
   * @param value the value of the property
   */
  public void setProperty(String property, String value) {
    properties.put(property, value);
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

  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties);
  }

  @Override
  public String toString() {
    return "Scan{" +
      "startRow=" + Bytes.toStringBinary(startRow) +
      ", stopRow=" + Bytes.toStringBinary(stopRow) +
      ", filter=" + filter +
      ", properties=" + properties +
      '}';
  }
}

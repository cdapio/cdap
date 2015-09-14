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

package org.apache.hadoop.hbase.regionserver;


/**
 * This is a reader class for {@link ScannerContext}. It allows to read the limits used to create the ScannerContext.
 * This is needed because IncrementSummingScanner fo HBase 1.1 needs to access the batch limit from the
 * {@link ScannerContext}.
 */
public class ScannerContextReader {

  private ScannerContext scannerContext;

  public ScannerContextReader(ScannerContext scannerContext) {
    this.scannerContext = scannerContext;
  }

  public ScannerContext getScannerContext() {
    return scannerContext;
  }

  public int getBatchLimit() {
    return scannerContext.getBatchLimit();
  }

  public long getSizeLimit() {
    return scannerContext.getSizeLimit();
  }

  public long getTimeLimit() {
    return scannerContext.getTimeLimit();
  }
}

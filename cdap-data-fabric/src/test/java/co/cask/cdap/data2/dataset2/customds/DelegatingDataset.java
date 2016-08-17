/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.customds;

import co.cask.cdap.api.dataset.Dataset;

import java.io.IOException;

/**
 *
 */
public class DelegatingDataset implements Dataset, CustomOperations {

  private final CustomOperations delegate;

  public DelegatingDataset(CustomOperations delegate) {
    this.delegate = delegate;
  }

  @Override
  public void read() {
    delegate.read();
  }

  @Override
  public void write() {
    delegate.write();
  }

  @Override
  public void readWrite() {
    delegate.readWrite();
  }

  @Override
  public void lineageWriteActualReadWrite() {
    delegate.lineageWriteActualReadWrite();
  }

  @Override
  public void noDataOp() {
    delegate.noDataOp();
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}

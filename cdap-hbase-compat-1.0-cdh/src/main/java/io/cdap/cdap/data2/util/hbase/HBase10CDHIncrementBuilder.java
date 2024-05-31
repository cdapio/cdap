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

package io.cdap.cdap.data2.util.hbase;

/**
 * HBase 1.0 specific implementation for {@link PutBuilder}.
 */
class HBase10CDHIncrementBuilder extends DefaultIncrementBuilder {

  HBase10CDHIncrementBuilder(byte[] row) {
    super(row);
  }

  @Override
  public IncrementBuilder setAttribute(String name, byte[] value) {
    increment.setAttribute(name, value);
    return this;
  }
}

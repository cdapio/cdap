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

package co.cask.cdap.data2.util.hbase;

import org.apache.hadoop.hbase.client.Delete;

/**
 * Default implementation of {@link DeleteBuilder}. Specific HBase compat module can extends and override methods.
 */
class DefaultDeleteBuilder implements DeleteBuilder {

  protected final Delete delete;

  DefaultDeleteBuilder(byte[] row) {
    delete = new Delete(row);
  }

  DefaultDeleteBuilder(Delete delete) {
    this.delete = new Delete(delete);
  }

  @Override
  public DeleteBuilder deleteFamily(byte[] family) {
    delete.deleteFamily(family);
    return this;
  }

  @Override
  public DeleteBuilder deleteFamily(byte[] family, long timestamp) {
    delete.deleteFamily(family, timestamp);
    return this;
  }

  @Override
  public DeleteBuilder deleteFamilyVersion(byte[] family, long timestamp) {
    delete.deleteFamilyVersion(family, timestamp);
    return this;
  }

  @Override
  public DeleteBuilder deleteColumns(byte[] family, byte[] qualifier) {
    delete.deleteColumns(family, qualifier);
    return this;
  }

  @Override
  public DeleteBuilder deleteColumns(byte[] family, byte[] qualifier, long timestamp) {
    delete.deleteColumns(family, qualifier, timestamp);
    return this;
  }

  @Override
  public DeleteBuilder deleteColumn(byte[] family, byte[] qualifier) {
    delete.deleteColumn(family, qualifier);
    return this;
  }

  @Override
  public DeleteBuilder deleteColumn(byte[] family, byte[] qualifier, long timestamp) {
    delete.deleteColumn(family, qualifier, timestamp);
    return this;
  }

  @Override
  public DeleteBuilder setTimestamp(long timestamp) {
    delete.setTimestamp(timestamp);
    return this;
  }

  @Override
  public DeleteBuilder setAttribute(String name, byte[] value) {
    delete.setAttribute(name, value);
    return this;
  }

  @Override
  public DeleteBuilder setId(String id) {
    delete.setId(id);
    return this;
  }

  @Override
  public boolean isEmpty() {
    return delete.isEmpty();
  }

  @Override
  public Delete build() {
    return delete;
  }

  @Override
  public String toString() {
    return delete.toString();
  }
}

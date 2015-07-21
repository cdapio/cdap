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

import org.apache.hadoop.hbase.client.Put;

import java.nio.ByteBuffer;

/**
 * Default implementation of {@link PutBuilder}. Specific HBase compat module can extends and override methods.
 */
class DefaultPutBuilder implements PutBuilder {

  protected final Put put;

  DefaultPutBuilder(byte[] row) {
    this.put = new Put(row);
  }
  
  DefaultPutBuilder(Put put) {
    this.put = new Put(put);
  }

  @Override
  public PutBuilder add(byte[] family, byte[] qualifier, byte[] value) {
    put.add(family, qualifier, value);
    return this;
  }

  @Override
  public PutBuilder addImmutable(byte[] family, byte[] qualifier, byte[] value) {
    put.addImmutable(family, qualifier, value);
    return this;
  }

  @Override
  public PutBuilder add(byte[] family, byte[] qualifier, long ts, byte[] value) {
    put.add(family, qualifier, ts, value);
    return this;
  }

  @Override
  public PutBuilder addImmutable(byte[] family, byte[] qualifier, long ts, byte[] value) {
    put.addImmutable(family, qualifier, ts, value);
    return this;
  }

  @Override
  public PutBuilder add(byte[] family, ByteBuffer qualifier, long ts, ByteBuffer value) {
    put.add(family, qualifier, ts, value);
    return this;
  }

  @Override
  public PutBuilder addImmutable(byte[] family, ByteBuffer qualifier, long ts, ByteBuffer value) {
    put.addImmutable(family, qualifier, ts, value);
    return this;
  }

  @Override
  public PutBuilder setAttribute(String name, byte[] value) {
    put.setAttribute(name, value);
    return this;
  }

  @Override
  public PutBuilder setId(String id) {
    put.setId(id);
    return this;
  }

  @Override
  public boolean isEmpty() {
    return put.isEmpty();
  }

  @Override
  public Put build() {
    return put;
  }

  @Override
  public String toString() {
    return put.toString();
  }
}

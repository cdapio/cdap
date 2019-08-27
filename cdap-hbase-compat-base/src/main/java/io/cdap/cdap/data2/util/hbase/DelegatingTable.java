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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A concrete class implementation that delegate all {@link Table} operations to another {@link Table}.
 */
public class DelegatingTable implements Table {

  private final Table delegate;

  public DelegatingTable(Table delegate) {
    this.delegate = delegate;
  }

  /**
   * Returns the underlying {@link Table} for delegation.
   */
  public Table getDelegate() {
    return delegate;
  }

  @Override
  public TableName getName() {
    return getDelegate().getName();
  }

  @Override
  public Configuration getConfiguration() {
    return getDelegate().getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return getDelegate().getTableDescriptor();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    return getDelegate().exists(get);
  }

  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    return getDelegate().existsAll(gets);
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
    getDelegate().batch(actions, results);
  }

  @Override
  @Deprecated
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    return getDelegate().batch(actions);
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
                                Batch.Callback<R> callback) throws IOException, InterruptedException {
    getDelegate().batchCallback(actions, results, callback);
  }

  @Override
  @Deprecated
  public <R> Object[] batchCallback(List<? extends Row> actions,
                                    Batch.Callback<R> callback) throws IOException, InterruptedException {
    return getDelegate().batchCallback(actions, callback);
  }

  @Override
  public Result get(Get get) throws IOException {
    return getDelegate().get(get);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return getDelegate().get(gets);
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return getDelegate().getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return getDelegate().getScanner(family);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return getDelegate().getScanner(family, qualifier);
  }

  @Override
  public void put(Put put) throws IOException {
    getDelegate().put(put);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    getDelegate().put(puts);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
    return getDelegate().checkAndPut(row, family, qualifier, value, put);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
                             CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
    return getDelegate().checkAndPut(row, family, qualifier, compareOp, value, put);
  }

  @Override
  public void delete(Delete delete) throws IOException {
    getDelegate().delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    getDelegate().delete(deletes);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family,
                                byte[] qualifier, byte[] value, Delete delete) throws IOException {
    return getDelegate().checkAndDelete(row, family, qualifier, value, delete);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
                                CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {
    return getDelegate().checkAndDelete(row, family, qualifier, compareOp, value, delete);
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    getDelegate().mutateRow(rm);
  }

  @Override
  public Result append(Append append) throws IOException {
    return getDelegate().append(append);
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return getDelegate().increment(increment);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
    return getDelegate().incrementColumnValue(row, family, qualifier, amount);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family,
                                   byte[] qualifier, long amount, Durability durability) throws IOException {
    return getDelegate().incrementColumnValue(row, family, qualifier, amount, durability);
  }

  @Override
  public void close() throws IOException {
    getDelegate().close();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return getDelegate().coprocessorService(row);
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
                                                                  Batch.Call<T, R> callable) throws Throwable {
    return getDelegate().coprocessorService(service, startKey, endKey, callable);
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
                                                        Batch.Call<T, R> callable,
                                                        Batch.Callback<R> callback) throws Throwable {
    getDelegate().coprocessorService(service, startKey, endKey, callable, callback);
  }

  @Override
  @Deprecated
  public long getWriteBufferSize() {
    return getDelegate().getWriteBufferSize();
  }

  @Override
  @Deprecated
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    getDelegate().setWriteBufferSize(writeBufferSize);
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                                    Message request, byte[] startKey, byte[] endKey,
                                                                    R responsePrototype) throws Throwable {
    return getDelegate().batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
  }

  @Override
  public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                          Message request, byte[] startKey, byte[] endKey,
                                                          R responsePrototype,
                                                          Batch.Callback<R> callback) throws Throwable {
    getDelegate().batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype, callback);
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
                                byte[] value, RowMutations mutation) throws IOException {
    return getDelegate().checkAndMutate(row, family, qualifier, compareOp, value, mutation);
  }
}

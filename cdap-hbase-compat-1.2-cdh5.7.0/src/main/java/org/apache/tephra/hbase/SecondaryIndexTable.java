/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tephra.hbase;

import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.distributed.TransactionServiceClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Transactional SecondaryIndexTable.
 */
public class SecondaryIndexTable implements Closeable {
  private byte[] secondaryIndex;
  private TransactionAwareHTable transactionAwareHTable;
  private TransactionAwareHTable secondaryIndexTable;
  private TransactionContext transactionContext;
  private final TableName secondaryIndexTableName;
  private static final byte[] secondaryIndexFamily = Bytes.toBytes("secondaryIndexFamily");
  private static final byte[] secondaryIndexQualifier = Bytes.toBytes('r');
  private static final byte[] DELIMITER  = new byte[] {0};

  public SecondaryIndexTable(TransactionServiceClient transactionServiceClient, HTableInterface hTable,
                             byte[] secondaryIndex) {
    secondaryIndexTableName = TableName.valueOf(hTable.getName().getNameAsString() + ".idx");
    HTable secondaryIndexHTable = null;
    try (HBaseAdmin hBaseAdmin = new HBaseAdmin(hTable.getConfiguration())) {
      if (!hBaseAdmin.tableExists(secondaryIndexTableName)) {
        hBaseAdmin.createTable(new HTableDescriptor(secondaryIndexTableName));
      }
      secondaryIndexHTable = new HTable(hTable.getConfiguration(), secondaryIndexTableName);
    } catch (Exception e) {
      Throwables.propagate(e);
    }

    this.secondaryIndex = secondaryIndex;
    this.transactionAwareHTable = new TransactionAwareHTable(hTable);
    this.secondaryIndexTable = new TransactionAwareHTable(secondaryIndexHTable);
    this.transactionContext = new TransactionContext(transactionServiceClient, transactionAwareHTable,
                                                     secondaryIndexTable);
  }

  public Result get(Get get) throws IOException {
    return get(Collections.singletonList(get))[0];
  }

  public Result[] get(List<Get> gets) throws IOException {
    try {
      transactionContext.start();
      Result[] result = transactionAwareHTable.get(gets);
      transactionContext.finish();
      return result;
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction", e1);
      }
    }
    return null;
  }

  public Result[] getByIndex(byte[] value) throws IOException {
    try {
      transactionContext.start();
      Scan scan = new Scan(value, Bytes.add(value, new byte[0]));
      scan.addColumn(secondaryIndexFamily, secondaryIndexQualifier);
      ResultScanner indexScanner = secondaryIndexTable.getScanner(scan);

      ArrayList<Get> gets = new ArrayList<>();
      for (Result result : indexScanner) {
        for (Cell cell : result.listCells()) {
          gets.add(new Get(cell.getValue()));
        }
      }
      Result[] results = transactionAwareHTable.get(gets);
      transactionContext.finish();
      return results;
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction", e1);
      }
    }
    return null;
  }

  public void put(Put put) throws IOException {
    put(Collections.singletonList(put));
  }


  public void put(List<Put> puts) throws IOException {
    try {
      transactionContext.start();
      ArrayList<Put> secondaryIndexPuts = new ArrayList<>();
      for (Put put : puts) {
        List<Put> indexPuts = new ArrayList<>();
        Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
        for (Map.Entry<byte [], List<KeyValue>> family : familyMap) {
          for (KeyValue value : family.getValue()) {
            if (Bytes.equals(value.getQualifierArray(), value.getQualifierOffset(), value.getQualifierLength(),
                             secondaryIndex, 0, secondaryIndex.length)) {
              byte[] secondaryRow = Bytes.add(value.getQualifier(), DELIMITER,
                                                    Bytes.add(value.getValue(), DELIMITER,
                                                              value.getRow()));
              Put indexPut = new Put(secondaryRow);
              indexPut.add(secondaryIndexFamily, secondaryIndexQualifier, put.getRow());
              indexPuts.add(indexPut);
            }
          }
        }
        secondaryIndexPuts.addAll(indexPuts);
      }
      transactionAwareHTable.put(puts);
      secondaryIndexTable.put(secondaryIndexPuts);
      transactionContext.finish();
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction", e1);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      transactionAwareHTable.close();
    } catch (IOException e) {
      try {
        secondaryIndexTable.close();
      } catch (IOException ex) {
        e.addSuppressed(e);
      }
      throw e;
    }
    secondaryIndexTable.close();
  }
}

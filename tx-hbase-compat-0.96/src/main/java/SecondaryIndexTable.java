/*
 * Copyright 2014 Continuuity, Inc.
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

import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.inmemory.DetachedTxSystemClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Transactional SecondaryIndexTable.
 */
public class SecondaryIndexTable {
  private byte[] secondaryIndex;
  private byte[] secondaryIndexFamily;
  private TransactionAwareHTable transactionAwareHTable;
  private TransactionAwareHTable secondaryIndexTable;
  private TransactionContext transactionContext;

  public SecondaryIndexTable(HTable hTable, HTable secondaryTable, byte[] secondaryIndex) {
    this.secondaryIndex = secondaryIndex;
    this.secondaryIndexFamily = Bytes.toBytes("indexFamily");
    this.transactionAwareHTable = new TransactionAwareHTable(hTable);
    this.secondaryIndexTable = new TransactionAwareHTable(secondaryTable);
    this.transactionContext = new TransactionContext(new DetachedTxSystemClient(), transactionAwareHTable,
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
        throw new IOException("Could not rollback transaction");
      }
    }
    return null;
  }

  public Result[] getByIndex(byte[] value) throws IOException {
    try {
      transactionContext.start();
      Scan scan = new Scan(value, Bytes.add(value, new byte[0]));
      ResultScanner indexScanner = secondaryIndexTable.getScanner(scan);

      ArrayList<Get> gets = new ArrayList<Get>();
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
        throw new IOException("Could not rollback transaction");
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
      ArrayList<Put> secondaryIndexPuts = new ArrayList<Put>();
      for (Put put : puts) {
        Put secondaryIndexPut = new Put(put.getRow());
        Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
        for (Map.Entry<byte [], List<KeyValue>> family : familyMap) {
          for (KeyValue value : family.getValue()) {
            if (value.getQualifier().equals(secondaryIndex)) {
              byte[] secondaryQualifier = Bytes.add(Bytes.add(value.getQualifier(), new byte[0], value.getValue()),
                                                    new byte[0], value.getRow());
              secondaryIndexPut.add(secondaryIndexFamily, secondaryQualifier, value.getValue());
            }
          }
        }
        secondaryIndexPuts.add(secondaryIndexPut);
      }
      transactionAwareHTable.put(puts);
      secondaryIndexTable.put(secondaryIndexPuts);
      transactionContext.finish();
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction");
      }
    }
  }
}

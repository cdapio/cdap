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

package org.apache.tephra.persist;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

/**
 * {@link TransactionLogReader} that can read v1 (default) version of Transaction logs. The logs are expected to
 * have a sequence of {@link TransactionEdit}s.
 */
public class HDFSTransactionLogReaderV1 implements TransactionLogReader {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionLogReaderV1.class);
  private final SequenceFile.Reader reader;
  private final LongWritable key;
  private boolean closed;

  public HDFSTransactionLogReaderV1(SequenceFile.Reader reader) {
    this.reader = reader;
    this.key = new LongWritable();
  }

  @Override
  public TransactionEdit next() throws IOException {
    return next(new TransactionEdit());
  }

  @Override
  public TransactionEdit next(TransactionEdit reuse) throws IOException {
    if (closed) {
      return null;
    }

    try {
      co.cask.tephra.persist.TransactionEdit oldTxEdit = new co.cask.tephra.persist.TransactionEdit();
      boolean successful = reader.next(key, oldTxEdit);
      return successful ? TransactionEdit.convertCaskTxEdit(oldTxEdit) : null;
    } catch (EOFException e) {
      LOG.warn("Hit an unexpected EOF while trying to read the Transaction Edit. Skipping the entry.", e);
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    reader.close();
    closed = true;
  }
}

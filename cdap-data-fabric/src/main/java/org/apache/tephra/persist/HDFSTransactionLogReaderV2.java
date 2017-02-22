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
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * {@link TransactionLogReader} that can read v2 version of Transaction logs. The logs are expected to
 * have commit markers that indicates the size of the batch of {@link TransactionEdit}s (follows the marker),
 * that were synced together. If the expected number of {@link TransactionEdit}s are not present then that set of
 * {@link TransactionEdit}s are discarded.
 */
public class HDFSTransactionLogReaderV2 implements TransactionLogReader {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionLogReaderV2.class);

  private final SequenceFile.Reader reader;
  private final Queue<co.cask.tephra.persist.TransactionEdit> transactionEdits;
  private final CommitMarkerCodec commitMarkerCodec;
  private final LongWritable key;

  private boolean closed;

  public HDFSTransactionLogReaderV2(SequenceFile.Reader reader) {
    this.reader = reader;
    this.transactionEdits = new ArrayDeque<>();
    this.key = new LongWritable();
    this.commitMarkerCodec = new CommitMarkerCodec();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      commitMarkerCodec.close();
    } finally {
      reader.close();
      closed = true;
    }
  }

  @Override
  public TransactionEdit next() throws IOException {
    return next(null);
  }

  @Override
  public TransactionEdit next(TransactionEdit reuse) throws IOException {
    if (closed) {
      return null;
    }

    if (!transactionEdits.isEmpty()) {
      return TransactionEdit.convertCaskTxEdit(transactionEdits.remove());
    }

    // Fetch the 'marker' and read 'marker' number of edits
    populateTransactionEdits();
    return TransactionEdit.convertCaskTxEdit(transactionEdits.poll());
  }

  private void populateTransactionEdits() throws IOException {
    // read the marker to determine numEntries to read.
    int numEntries = 0;
    try {
      // can throw EOFException if reading of incomplete commit marker, no other action required since we can safely
      // ignore this
      numEntries = commitMarkerCodec.readMarker(reader);
    } catch (EOFException e) {
      LOG.warn("Reached EOF in log while trying to read commit marker", e);
    }

    for (int i = 0; i < numEntries; i++) {
      co.cask.tephra.persist.TransactionEdit edit = new co.cask.tephra.persist.TransactionEdit();
      try {
        if (reader.next(key, edit)) {
          transactionEdits.add(edit);
        } else {
          throw new EOFException("Attempt to read TransactionEdit failed.");
        }
      } catch (EOFException e) {
        // we have reached EOF before reading back numEntries, we clear the partial list and return.
        LOG.warn("Reached EOF in log before reading {} entries. Ignoring all {} edits since the last marker",
                 numEntries, transactionEdits.size(), e);
        transactionEdits.clear();
      }
    }
  }
}

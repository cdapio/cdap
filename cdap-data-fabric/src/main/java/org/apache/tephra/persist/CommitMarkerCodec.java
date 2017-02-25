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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.tephra.TxConstants;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Class to read and write commit markers used in {@link HDFSTransactionLogReaderV2} and above.
 */
public class CommitMarkerCodec implements Closeable {
  private static final byte[] KEY_BYTES = TxConstants.TransactionLog.NUM_ENTRIES_APPENDED.getBytes(Charsets.UTF_8);
  private final DataOutputBuffer rawKey;
  private final DataOutputBuffer rawValue;
  private SequenceFile.ValueBytes valueBytes;

  public CommitMarkerCodec() {
    this.rawKey = new DataOutputBuffer();
    this.rawValue = new DataOutputBuffer();
  }

  @Override
  public void close() throws IOException {
    rawKey.close();
    rawValue.close();
  }

  // 1. Returns the count when the marker is written correctly
  // 2. If data is incorrect (for ex, incorrect key, mismatch in key/value/record length), we throw IOException
  // since this indicates corrupted log file
  // 3. If data is incomplete, then we throw EOFException which is handled gracefully by the calling method
  // since we can recover without any consequence
  public int readMarker(SequenceFile.Reader reader) throws IOException {
    if (valueBytes == null) {
      valueBytes = reader.createValueBytes();
    }
    rawKey.reset();
    rawValue.reset();

    // valueBytes need not be reset since nextRaw call does it (and it is a private method)
    int status = reader.nextRaw(rawKey, valueBytes);

    // if we reach EOF, return -1
    if (status == -1) {
      return -1;
    }

    // Check if the marker key is valid and return the count
    if (isMarkerValid()) {
      valueBytes.writeUncompressedBytes(rawValue);
      rawValue.flush();
      // rawValue.getData() may return a larger byte array but Ints.fromByteArray will only read the first four bytes
      return Ints.fromByteArray(rawValue.getData());
    }

    // EOF not reached and marker is not valid, then thrown an IOException since we can't make progress
    throw new IOException(String.format("Invalid key for num entries appended found %s, expected : %s",
                                        new String(rawKey.getData()), TxConstants.TransactionLog.NUM_ENTRIES_APPENDED));
  }

  private boolean isMarkerValid() {
    // rawKey should have the expected length and the matching bytes should start at index 0
    return rawKey.getLength() == KEY_BYTES.length && Bytes.indexOf(rawKey.getData(), KEY_BYTES) == 0;
  }

  public static void writeMarker(SequenceFile.Writer writer, int count) throws IOException {
    writer.appendRaw(KEY_BYTES, 0, KEY_BYTES.length, new CommitEntriesCount(count));
  }

  @VisibleForTesting
  static final class CommitEntriesCount implements SequenceFile.ValueBytes {
    private final int numEntries;

    public CommitEntriesCount(int numEntries) {
      this.numEntries = numEntries;
    }

    @Override
    public void writeUncompressedBytes(DataOutputStream outStream) throws IOException {
      outStream.write(Ints.toByteArray(numEntries));
    }

    @Override
    public void writeCompressedBytes(DataOutputStream outStream) throws IllegalArgumentException, IOException {
      throw new IllegalArgumentException("Commit Entries count writing is not expected to be compressed.");
    }

    @Override
    public int getSize() {
      return Ints.BYTES;
    }
  }
}

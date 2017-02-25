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

import org.apache.tephra.metrics.MetricsCollector;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Reads and writes transaction logs against files in the local filesystem.
 */
public class LocalFileTransactionLog extends AbstractTransactionLog {
  private final File logFile;

  /**
   * Creates a new transaction log using the given file instance.
   * @param logFile The log file to use.
   */
  public LocalFileTransactionLog(File logFile, long timestamp, MetricsCollector metricsCollector) {
    super(timestamp, metricsCollector);
    this.logFile = logFile;
  }

  @Override
  public String getName() {
    return logFile.getAbsolutePath();
  }

  @Override
  protected TransactionLogWriter createWriter() throws IOException {
    return new LogWriter(logFile);
  }

  @Override
  public TransactionLogReader getReader() throws IOException {
    return new LogReader(logFile);
  }

  private static final class LogWriter implements TransactionLogWriter {
    private final FileOutputStream fos;
    private final DataOutputStream out;

    public LogWriter(File logFile) throws IOException {
      this.fos = new FileOutputStream(logFile);
      this.out = new DataOutputStream(new BufferedOutputStream(fos, LocalFileTransactionStateStorage.BUFFER_SIZE));
    }

    @Override
    public void append(Entry entry) throws IOException {
      entry.write(out);
    }

    @Override
    public void commitMarker(int count) throws IOException {
      // skip for local file
    }

    @Override
    public void sync() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      out.flush();
      out.close();
      fos.close();
    }
  }

  private static final class LogReader implements TransactionLogReader {
    private final FileInputStream fin;
    private final DataInputStream in;
    private Entry reuseEntry = new Entry();

    public LogReader(File logFile) throws IOException {
      this.fin = new FileInputStream(logFile);
      this.in = new DataInputStream(new BufferedInputStream(fin, LocalFileTransactionStateStorage.BUFFER_SIZE));
    }

    @Override
    public TransactionEdit next() throws IOException {
      Entry entry = new Entry();
      try {
        entry.readFields(in);
      } catch (EOFException eofe) {
        // signal end of file by returning null
        return null;
      }
      return entry.getEdit();
    }

    @Override
    public TransactionEdit next(TransactionEdit reuse) throws IOException {
      try {
        reuseEntry.getKey().readFields(in);
        reuse.readFields(in);
      } catch (EOFException eofe) {
        // signal end of file by returning null
        return null;
      }
      return reuse;
    }

    @Override
    public void close() throws IOException {
      in.close();
      fin.close();
    }
  }
}

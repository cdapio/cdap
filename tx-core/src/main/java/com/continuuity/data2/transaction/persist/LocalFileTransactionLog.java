package com.continuuity.data2.transaction.persist;

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
  public LocalFileTransactionLog(File logFile, long timestamp) {
    super(timestamp);
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

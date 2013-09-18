package com.continuuity.data2.transaction.persist;

import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class LocalFileTransactionStateStorage extends InMemoryTransactionStateStorage {
  /*
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileTransactionStateStorage.class);

  @Override
  protected void startUp() throws Exception {
    // TODO
    // create temporary directory
  }

  @Override
  protected void shutDown() throws Exception {
    // clear out temporary directory?
  }

  @Override
  public void writeSnapshot(TransactionSnapshot snapshot) throws IOException {
    // TODO: instead of making an extra in-memory copy, serialize the snapshot directly to the file output stream
    SnapshotCodec codec = new SnapshotCodec();
    byte[] serialized = codec.encodeState(snapshot);
    // create a temporary file, and save the snapshot
  }

  @Override
  public TransactionSnapshot getLatestSnapshot() throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<TransactionLog> getLogsSince(long timestamp) throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public TransactionLog createLog(long timestamp) throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  private static class LocalFileTransactionLog implements TransactionLog {
    @Override
    public void append(TransactionEdit edit) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void append(List<TransactionEdit> edits) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TransactionLogReader getReader() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
  }
  */
}

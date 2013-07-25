package com.continuuity.data2.tx;

import java.util.Collection;

/**
 *
 */
// todo: use custom exception class?
// todo: review exception handling where it is used
public interface TxAware {
  // should not keep state outside tx lifecycle
  void startTx(Tx tx);

  // returns changes made by current transaction (used to detect conflicts before commit)
  Collection<byte[]> getTxChanges();

  // commit current tx
  boolean commitTx() throws Exception;

  // undo changes made by tx
  // NOTE: borted tx will only be removed from the excluded list when all datasets reported successful rollback
  boolean abortTx() throws Exception;

  // todo: add flush?
  // todo: add onCommitted() - so that e.g. hbase table can do *actual* deletes at this point
}

package com.continuuity.data2.transaction;

import java.util.Collection;

/**
 * Interface to be implemented by a component that interacts with transaction logic.
 * See also {@link TransactionSystemClient}.
 * <p/>
 * The client code that uses transaction logic looks like this:
 * <pre>
 *  TransactionAware dataSet = // ...              // dataSet is one example of component that interacts with tx logic
 *
 *  Transaction tx = txClient.start();
 *  dataSet.startTx(tx);                           // notifying about new transaction
 *  dataSet.write(...);
 *  // ... do other operations on dataSet
 *  Collection<byte[]> changes = dataSet.getTxChanges();
 *  if (txClient.canCommit(changes)) {             // checking conflicts before commit, if none, commit tx
 *    if (dataSet.commitTx()) {                    // try persisting changes
 *      txClient.commit(tx);                       // if OK, make tx visible; if not - tx stays invisible to others
 *    }
 *  } else {                                       // if there are conflicts (or cannot commit), try rollback changes
 *    if (dataSet.rollbackTx()) {                   // try undo changes
 *      txClient.abort(tx);                        // if OK, make tx visible; if not - tx stays invisible to others
 *    }
 *  }
 *
 * </pre>
 */
// todo: use custom exception class?
// todo: review exception handling where it is used
// todo: add flush()? nah - flush is same as commitTx() actually
// todo: add onCommitted() - so that e.g. hbase table can do *actual* deletes at this point
public interface TransactionAware {
  /**
   * Called when new transaction has started
   * @param tx transaction info
   */
  // todo: rename to onTxStart()
  void startTx(Transaction tx);

  /**
   * @return changes made by current transaction to be used for conflicts detection before commit
   */
  Collection<byte[]> getTxChanges();

  /**
   * Called before transaction has been committed.
   * Can be used e.g. to flush changes cached in-memory to persistent store.
   * @return true if transaction can be committed, otherwise false.
   */
  // todo: rename to beforeTxCommit()
  boolean commitTx() throws Exception;

  /**
   * Called during transaction rollback (for whatever reason: conflicts, errors, etc.).
   * @return true if all changes made during transaction were rolled back, false otherwise (e.g. if more cleanup needed
   *         or changes cannot be undone). True also means that this transaction can be made visible to others without
   *         breaking consistency of the data: since all changes were undone there's "nothing to see".
   */
  // todo: rename to onTxRollback()
  boolean rollbackTx() throws Exception;
}

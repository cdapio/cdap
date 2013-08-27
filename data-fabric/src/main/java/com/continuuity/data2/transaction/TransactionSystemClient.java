package com.continuuity.data2.transaction;

import java.util.Collection;

/**
 * Client talking to transaction system.
 * See also {@link TransactionAware}.
 * todo: explain Omid.
 */
public interface TransactionSystemClient {
  /**
   * Starts new transaction.
   * @return instance of {@link Transaction}
   */
  Transaction start();

  // this pre-commit detects conflicts with other transactions committed so far
  // NOTE: the changes set should not change after this operation, this may help us do some extra optimizations
  // NOTE: there should be time constraint on how long does it take to commit changes by the client after this operation
  //       is submitted so that we can cleanup related resources

  /**
   * Checks if transaction with the set of changes can be committed. E.g. it can check conflicts with other changes and
   * refuse commit if there are conflicts. It is assumed that no other changes will be done in between this method call
   * and {@link #commit(Transaction)} which may check conflicts again to avoid races.
   * <p/>
   * Since we do conflict detection at commit time as well, this may seem redundant. The idea is to check for conflicts
   * before we persist changes to avoid rollback in case of conflicts as much as possible.
   * NOTE: in some situations we may want to skip this step to save on RPC with a risk of many rollback ops. So by
   *       default we take safe path.
   *
   * @param tx transaction to verify
   * @param changeIds ids of changes made by transaction
   * @return true if transaction can be committed otherwise false
   */
  boolean canCommit(Transaction tx, Collection<byte[]> changeIds);

  // this is called to make tx changes visible (i.e. removes it from excluded list) after all changes are committed by
  // client
  // todo: can it return false

  /**
   * Makes transaction visible. It will again check conflicts of changes submitted previously with
   * {@link #canCommit(Transaction, java.util.Collection)}
   * @param tx transaction to make visible.
   * @return
   */
  boolean commit(Transaction tx);

  /**
   * Makes transaction visible. You should call it only when all changes of this tx are undone.
   * @param tx transaction to make visible.
   * @return
   */
  boolean abort(Transaction tx);
}

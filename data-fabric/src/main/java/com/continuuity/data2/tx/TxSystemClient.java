package com.continuuity.data2.tx;

import java.util.Collection;

/**
 *
 */
public interface TxSystemClient {

  Tx start();

  // this pre-commit detects conflicts with other transactions committed so far
  // NOTE: the changes set should not change after this operation, this may help us do some extra optimizations
  // NOTE: there should be time constraint on how long does it take to commit changes by the client after this operation
  //       is submitted so that we can cleanup related resources
  boolean hasConflicts(Tx tx, Collection<byte[]> changeIds);

  // this is called to make tx changes visible (i.e. removes it from excluded list) after all changes are committed by
  // client
  // todo: can it return false
  boolean makeVisible(Tx tx);
}

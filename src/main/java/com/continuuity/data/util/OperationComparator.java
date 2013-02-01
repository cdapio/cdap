package com.continuuity.data.util;

import java.util.Comparator;

import com.continuuity.data.operation.Operation;

/**
 * Comparator for Operations.
 * <p>
 * Uses the unique ID of the operation.  Can be used to make TreeMaps with an
 * Operation as the key.
 * @param <T> 
 */
public class OperationComparator<T extends Operation> implements Comparator<T> {

  @Override
  public int compare(Operation left, Operation right) {
    return (int)(left.getId() - right.getId());
  }

}

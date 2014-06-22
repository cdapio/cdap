package com.continuuity.data2.dataset2.tx;

/**
 * Function executed within transaction
 * @param <CONTEXT_TYPE> type of the {@link TxContext} given to the function {@link #call(TxContext)} method
 * @param <RETURN_TYPE> return type of the {@link #call(TxContext)} method
 */
public interface TxCallable<CONTEXT_TYPE extends TxContext, RETURN_TYPE> {
  RETURN_TYPE call(CONTEXT_TYPE context) throws Exception;
}

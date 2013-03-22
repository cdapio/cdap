package com.continuuity.api.data.batch;

import com.continuuity.api.data.OperationException;

public interface BatchWritable<KEY, VALUE> {
  void write(KEY key, VALUE value) throws OperationException;
}

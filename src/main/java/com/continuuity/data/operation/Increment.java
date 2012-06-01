package com.continuuity.data.operation;

import com.continuuity.data.operation.type.ReadOperation;
import com.continuuity.data.operation.type.WriteOperation;

public class Increment implements WriteOperation, ReadOperation<Long> {

  private byte [] key;
  private long amount;
  private long incrementedValue;
 
  private OperationGenerator<Long> postOperationGenerator;

  public Increment(byte [] key, long amount) {
    this.key = key;
    this.amount = amount;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }
 
  public long getAmount() {
    return this.amount;
  }

  @Override
  public void setResult(Long incrementedValue) {
    this.incrementedValue = incrementedValue;
  }

  @Override
  public Long getResult() {
    return this.incrementedValue;
  }

  public void setPostIncrementOperationGenerator(
      OperationGenerator<Long> postOperationGenerator) {
    this.postOperationGenerator = postOperationGenerator;
  }

  public OperationGenerator<Long> getPostIncrementOperationGenerator() {
    return this.postOperationGenerator;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}

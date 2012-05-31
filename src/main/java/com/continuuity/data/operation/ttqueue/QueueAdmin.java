package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.type.ReadOperation;
import com.google.common.base.Objects;

public class QueueAdmin {

  public static class GetGroupID implements ReadOperation<Long> {

    private long result;

    @Override
    public void setResult(Long result) {
      this.result = result;
    }

    @Override
    public Long getResult() {
      return this.result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("result", this.result)
          .toString();
    }

  }
}

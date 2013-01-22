package com.continuuity.data.operation;

import com.continuuity.api.data.Closure;
import com.continuuity.api.data.Increment;

public class IncrementClosure implements Closure {

  private Increment increment;

  public Increment getIncrement() {
    return increment;
  }

  public IncrementClosure(Increment increment) {
    this.increment = increment;
  }
}

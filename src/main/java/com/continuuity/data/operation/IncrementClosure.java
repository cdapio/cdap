package com.continuuity.data.operation;

import com.continuuity.api.data.Closure;
import com.continuuity.api.data.Increment;

/**
 * A closure that represents an increment
 */
public class IncrementClosure implements Closure {

  // the increment represented by this closure
  private Increment increment;

  /**
   * Get the increment represented buy this closure
   * @return the represented increment
   */
  public Increment getIncrement() {
    return increment;
  }

  /**
   * Constructor from an increment operation
   * @param increment The increment to represent
   */
  public IncrementClosure(Increment increment) {
    this.increment = increment;
  }
}

package com.continuuity.app.verification;

/**
 *
 */
public interface Stage<I, O> {
  public O process(final I data);
}

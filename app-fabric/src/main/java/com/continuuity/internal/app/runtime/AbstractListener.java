package com.continuuity.internal.app.runtime;

import com.continuuity.app.runtime.ProgramController;

/**
 * Base implementation of ProgramController.Listener that does nothing on any its method invocation.
 */
public abstract class AbstractListener implements ProgramController.Listener {

  @Override
  public void init(ProgramController.State currentState) {
  }

  @Override
  public void suspending() {
  }

  @Override
  public void suspended() {
  }

  @Override
  public void resuming() {
  }

  @Override
  public void alive() {
  }

  @Override
  public void stopping() {
  }

  @Override
  public void stopped() {
  }

  @Override
  public void error() {
  }
}

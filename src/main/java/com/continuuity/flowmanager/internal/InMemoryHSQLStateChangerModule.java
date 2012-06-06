package com.continuuity.flowmanager.internal;

import com.continuuity.flowmanager.StateChangeCallback;
import com.continuuity.flowmanager.SQLStateChangeSyncer;
import com.google.inject.AbstractModule;

/**
 *
 *
 */
public class InMemoryHSQLStateChangerModule extends AbstractModule {

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {
    bind(StateChangeCallback.class).to(SQLStateChangeSyncer.class);
  }
}

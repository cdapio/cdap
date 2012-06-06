package com.continuuity.overlord.flowmanager.internal;

import com.continuuity.overlord.flowmanager.SQLStateChangeSyncer;
import com.continuuity.overlord.flowmanager.StateChangeCallback;
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

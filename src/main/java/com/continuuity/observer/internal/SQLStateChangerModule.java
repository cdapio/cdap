package com.continuuity.observer.internal;

import com.continuuity.observer.StateChangeCallback;
import com.google.inject.AbstractModule;

/**
 *
 *
 */
public class SQLStateChangerModule extends AbstractModule {

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {
    bind(StateChangeCallback.class).to(SQLStateChangeSyncer.class);
  }
}

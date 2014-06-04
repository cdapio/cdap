/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a {@link PropertyChangeListener} that only log for callbacks.
 *
 * @param <T> Type of the property
 */
public abstract class AbstractPropertyChangeListener<T> implements PropertyChangeListener<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractPropertyChangeListener.class);

  @Override
  public void onChange(String name, T property) {
    LOG.info("onChange {} {}", name, property);
  }

  @Override
  public void onError(String name, Throwable failureCause) {
    LOG.error("onError {}", name, failureCause);
  }
}

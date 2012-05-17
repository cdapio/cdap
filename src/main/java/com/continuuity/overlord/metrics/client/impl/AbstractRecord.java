/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.Record;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  AbstractRecord provides the base functionalities.
 */
public abstract  class AbstractRecord implements Record {
  private static final Logger LOG =
    LoggerFactory.getLogger(AbstractRecord.class);
  /**
   *
   * @return
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(
      getName(),
      getDescription(),
      getTimestamp()
    );
  }

  /**
   *
   * @param obj
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    if(obj instanceof  AbstractRecord) {
      AbstractRecord other = (AbstractRecord) obj;
      return 
        Objects.equal(getName(), other.getName())  &&
        Objects.equal(getDescription(), other.getDescription()) &&
        Objects.equal(getTimestamp(), other.getTimestamp());
    }
    return false;
  }

  /**
   *
   * @return
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("timestamp", getTimestamp())
      .add("name", getName())
      .add("description", getDescription())
      .add("client", Iterables.toString(getMetrics()))
      .toString();
  }

}

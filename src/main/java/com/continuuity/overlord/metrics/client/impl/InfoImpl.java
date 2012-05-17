/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.Info;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 *
 */
public class InfoImpl implements Info {
  private static final Logger LOG = LoggerFactory.getLogger(InfoImpl.class);
  private final String name, description;

  public InfoImpl(String name, String description) {
    this.name = checkNotNull(name, "name");
    this.description = checkNotNull(description, "description");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override public boolean equals(Object obj) {
    if (obj instanceof Info) {
      Info other = (Info) obj;
      return Objects.equal(name, other.getName()) &&
        Objects.equal(description, other.getDescription());
    }
    return false;
  }

  @Override public int hashCode() {
    return Objects.hashCode(name, description);
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("description", description)
      .toString();
  }

}

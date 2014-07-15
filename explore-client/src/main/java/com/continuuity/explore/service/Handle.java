/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.explore.service;

import com.google.common.base.Objects;
import com.sun.org.apache.bcel.internal.generic.RETURN;

import java.util.UUID;

/**
 * Represents an operation that is submitted for execution to {@link Explore}.
 */
public class Handle {

  private static final String NO_OP_ID = "NO_OP";
  public static final Handle NO_OP = new Handle(NO_OP_ID);

  private final String handle;

  public static Handle generate() {
    // TODO: make sure handles are unique across multiple instances. - REACTOR-272
    return new Handle(UUID.randomUUID().toString());
  }

  public static Handle fromId(String id) {
    if (id.equals(NO_OP_ID)) {
      return NO_OP;
    }
    return new Handle(id);
  }

  private Handle(String handle) {
    this.handle = handle;
  }

  public String getHandle() {
    return handle;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", handle)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Handle that = (Handle) o;

    return Objects.equal(this.handle, that.handle);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(handle);
  }
}

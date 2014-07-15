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

package com.continuuity.app.deploy;

/**
 * A container class for holding configuration result.
 *
 * @param <T> Type of specification this result will hold.
 */
public class ConfigResult<T> {
  private final T specification;
  private final boolean status;
  private final String message;

  public ConfigResult(String message) {
    this(null, false, message);
  }

  public ConfigResult(T specification, boolean status, String message) {
    this.specification = specification;
    this.status = status;
    this.message = message;
  }

  public T getSpecification() {
    return specification;
  }

  public boolean getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }
}

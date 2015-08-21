/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common;

import co.cask.cdap.proto.Id;

/**
 * Thrown when an element is not found
 */
public class NotFoundException extends Exception {

  private final Object object;

  public NotFoundException(Object object, String objectString) {
    super(String.format("'%s' was not found", objectString));
    this.object = object;
  }

  public NotFoundException(Object object) {
    this(object, object.toString());
  }

  public NotFoundException(Id id) {
    this(id, id.getIdRep());
  }

  public Object getObject() {
    return object;
  }
}

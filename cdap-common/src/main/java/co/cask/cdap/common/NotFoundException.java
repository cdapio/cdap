/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.common.HttpErrorStatusProvider;
import co.cask.cdap.proto.id.EntityId;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;

/**
 * Thrown when an element is not found
 */
public class NotFoundException extends Exception implements HttpErrorStatusProvider {

  private final Object object;

  public NotFoundException(Object object, String objectString) {
    this(object, objectString, null);
  }

  public NotFoundException(Object object) {
    this(object, object.toString());
  }

  public NotFoundException(EntityId entityId) {
    this(entityId, entityId.toString());
  }

  public NotFoundException(Object object, String objectString, Throwable cause) {
    super(String.format("'%s' was not found.", objectString), cause);
    this.object = object;
  }

  public NotFoundException(String errorMessage) {
    super(errorMessage);
    this.object = null;
  }

  /**
   * Return the object which could not be found. Returns null, if the entity is not known, such as when thrown from
   * cdap-clients.
   */
  @Nullable
  public Object getObject() {
    return object;
  }

  @Override
  public int getStatusCode() {
    return HttpResponseStatus.NOT_FOUND.getCode();
  }
}

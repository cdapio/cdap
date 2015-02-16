/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.common.authorization.client;

import co.cask.common.authorization.IdentifiableObject;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import co.cask.common.authorization.UnauthorizedException;
import com.google.common.base.Function;

/**
 * No-op implementation of {@link AuthorizationClient}.
 */
public class NoopAuthorizationClient implements AuthorizationClient {

  @Override
  public void authorize(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                        Iterable<Permission> requiredPermissions) throws UnauthorizedException {

  }

  @Override
  public void authorize(ObjectId object, Iterable<SubjectId> subjects,
                        Iterable<Permission> requiredPermissions) throws UnauthorizedException {

  }

  @Override
  public boolean isAuthorized(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                              Iterable<Permission> requiredPermissions) {
    return true;
  }

  @Override
  public boolean isAuthorized(ObjectId object, Iterable<SubjectId> subjects,
                              Iterable<Permission> requiredPermissions) {
    return true;
  }

  @Override
  public <T extends IdentifiableObject> Iterable<T> filter(Iterable<T> objects,
                                                           Iterable<SubjectId> subjects,
                                                           Iterable<Permission> requiredPermissions) {
    return objects;
  }

  @Override
  public <T> Iterable<T> filter(Iterable<T> objects, Iterable<SubjectId> subjects,
                                Iterable<Permission> requiredPermissions, Function<T, ObjectId> tObjectIdFunction) {
    return objects;
  }
}

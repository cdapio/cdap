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
 * Provides ways to verify access to protected objects.
 */
public interface AuthorizationClient {

  /**
   * Verifies that at least one of the subjects has access to at least one of the objects
   * for all of the required permissions.
   *
   *
   * @param objects objects to consider
   * @param subjects subjects to consider
   * @param requiredPermissions permissions that are required
   * @throws UnauthorizedException if none of the subjects have access to the object
   *                               for all of the required permissions
   */
  public void authorize(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                        Iterable<Permission> requiredPermissions) throws UnauthorizedException;

  /**
   * Verifies that at least one of the subjects has access to the object for all of the required permissions.
   *
   *
   * @param object object to consider
   * @param subjects subjects to consider
   * @param requiredPermissions permissions that are required
   * @throws UnauthorizedException if none of the subjects have access to the object
   *                               for all of the required permissions
   */
  public void authorize(ObjectId object, Iterable<SubjectId> subjects,
                        Iterable<Permission> requiredPermissions) throws UnauthorizedException;

  /**
   * Indicates whether at least one of the subjects is allowed access to at least one of the objects
   * for all of the required permissions.
   *
   *
   * @param objects objects to consider
   * @param subjects subjects to consider
   * @param requiredPermissions permissions that are required
   * @return true if at least one of the subjects have access to at least one of the objects
   *         for all of the required permissions
   */
  public boolean isAuthorized(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                              Iterable<Permission> requiredPermissions);

  /**
   * Indicates whether at least one of the subjects is allowed access to the object for all of the required permissions.
   *
   * @param object object to consider
   * @param subjects subjects to consider
   * @param requiredPermissions permissions that are required
   * @return true if at least one of the subjects have access to the object for all of the required permissions
   */
  public boolean isAuthorized(ObjectId object, Iterable<SubjectId> subjects, Iterable<Permission> requiredPermissions);

  /**
   * Filters out objects that none of the subjectIds are allowed to access,
   * leaving only the objects that at least one of the subjectIds are allowed to access.
   *
   * Generally used for listing objects that a user has access to.
   */
  public <T extends IdentifiableObject> Iterable<T> filter(Iterable<T> objects,
                                                           Iterable<SubjectId> subjects,
                                                           Iterable<Permission> requiredPermissions);

  /**
   * Filters out objects that none of the subjectIds are allowed to access,
   * leaving only the objects that at least one of the subjectIds are allowed to access.
   *
   * Generally used for listing objects that a user has access to.
   */
  public <T> Iterable<T> filter(Iterable<T> objects,
                                Iterable<SubjectId> subjects,
                                Iterable<Permission> requiredPermissions,
                                Function<T, ObjectId> objectIdFunction);
}

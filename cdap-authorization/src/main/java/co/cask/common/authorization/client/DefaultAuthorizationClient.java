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

import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.IdentifiableObject;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import co.cask.common.authorization.UnauthorizedException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link AuthorizationClient} which uses {@link ACLStore}.
 */
public class DefaultAuthorizationClient implements AuthorizationClient {

  private final Supplier<ACLStore> aclStoreSupplier;

  /**
   * @param aclStoreSupplier used to set and get {@link co.cask.common.authorization.ACLEntry}s
   */
  @Inject
  public DefaultAuthorizationClient(ACLStoreSupplier aclStoreSupplier) {
    this.aclStoreSupplier = aclStoreSupplier;
  }

  public DefaultAuthorizationClient(ACLStore aclStore) {
    this.aclStoreSupplier = Suppliers.ofInstance(aclStore);
  }

  @Override
  public void authorize(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                        Iterable<Permission> requiredPermissions) throws UnauthorizedException {
    if (!isAuthorized(objects, subjects, requiredPermissions)) {
      throw new UnauthorizedException(objects, subjects, requiredPermissions);
    }
  }

  @Override
  public void authorize(ObjectId object, Iterable<SubjectId> subjects,
                        Iterable<Permission> requiredPermissions) throws UnauthorizedException {
    if (!isAuthorized(object, subjects, requiredPermissions)) {
      throw new UnauthorizedException(Collections.singleton(object), subjects, requiredPermissions);
    }
  }

  @Override
  public boolean isAuthorized(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                              Iterable<Permission> requiredPermissions) {

    try {
      // TODO: check isAuthorized of parent objects
      Set<Permission> remainingRequiredPermission = Sets.newHashSet(requiredPermissions);
      Set<ObjectId> visitedObjectIds = Sets.newHashSet();

      // TODO: go from most general (GLOBAL -> NAMESPACE -> APP)
      for (ObjectId object : objects) {
        // TODO: consider doing only a single aclStore call
        for (ObjectId parentObject : object.getParents()) {
          checkAuthorized(remainingRequiredPermission, visitedObjectIds, parentObject, subjects);
          if (remainingRequiredPermission.isEmpty()) {
            // Early exit if all permissions are fulfilled
            return true;
          }
        }

        checkAuthorized(remainingRequiredPermission, visitedObjectIds, object, subjects);
        if (remainingRequiredPermission.isEmpty()) {
          // Early exit if all permissions are fulfilled
          return true;
        }
      }
    } catch (Exception e) {
      // If any ACLStore calls failed, fall through to return false
    }

    return false;
  }

  private void checkAuthorized(Set<Permission> outRemainingRequiredPermission,
                               Set<ObjectId> visitedObjectIds,
                               ObjectId object, Iterable<SubjectId> subjects) throws Exception {
    if (!visitedObjectIds.contains(object)) {
      List<ACLStore.Query> queries = generateQueries(object, subjects, outRemainingRequiredPermission);
      Set<ACLEntry> aclEntries = getACLStore().search(queries);
      for (ACLEntry aclEntry : aclEntries) {
        outRemainingRequiredPermission.remove(aclEntry.getPermission());
        if (outRemainingRequiredPermission.isEmpty()) {
          return;
        }
      }
      visitedObjectIds.add(object);
    }
  }

  private List<ACLStore.Query> generateQueries(ObjectId object, Iterable<SubjectId> subjects,
                                               Iterable<Permission> permissions) {
    List<ACLStore.Query> queries = Lists.newArrayList();
    for (SubjectId subjectId : subjects) {
      for (Permission permission : permissions) {
        queries.add(new ACLStore.Query(object, subjectId, permission));
      }
    }
    return queries;
  }

  @Override
  public boolean isAuthorized(ObjectId object, Iterable<SubjectId> subjects,
                              Iterable<Permission> requiredPermissions) {

    return isAuthorized(ImmutableList.of(object), subjects, requiredPermissions);
  }

  @Override
  public <T extends IdentifiableObject> Iterable<T> filter(Iterable<T> objects,
                                                           final Iterable<SubjectId> subjects,
                                                           final Iterable<Permission> requiredPermissions) {
    // TODO: use only a single aclStore call
    return Iterables.filter(objects, new Predicate<IdentifiableObject>() {
      @Override
      public boolean apply(@Nullable IdentifiableObject input) {
        return input != null && isAuthorized(input.getObjectId(), subjects, requiredPermissions);
      }
    });
  }

  @Override
  public <T> Iterable<T> filter(Iterable<T> objects,
                                final Iterable<SubjectId> subjects,
                                final Iterable<Permission> requiredPermissions,
                                final Function<T, ObjectId> objectIdFunction) {
    // TODO: use only a single aclStore call
    return Iterables.filter(objects, new Predicate<T>() {
      @Override
      public boolean apply(@Nullable T input) {
        return input != null && isAuthorized(objectIdFunction.apply(input), subjects, requiredPermissions);
      }
    });
  }

  private ACLStore getACLStore() {
    ACLStore aclStore = aclStoreSupplier.get();
    Preconditions.checkNotNull(aclStore);
    return aclStore;
  }
}

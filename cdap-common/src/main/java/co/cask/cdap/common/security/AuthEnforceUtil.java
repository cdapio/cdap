/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.security;

import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;

import java.util.Arrays;
import java.util.Set;

/**
 * Class used by {@link AuthEnforceRewriter} to rewrite classes with {@link AuthEnforce} annotation and call
 * enforcement methods in this class to perform authorization enforcement.
 */
// Note: Do no remove the public modifier of this class. This class is marked public even though its only usage is
// from the package because after class rewrite AuthEnforce annotations are rewritten to make call to the methods
// in this class and since AuthEnforce annotation can be in any package after class rewrite this class methods
// might be being called from other packages.
public final class AuthEnforceUtil {

  private AuthEnforceUtil() {
    // no-op
  }

  /**
   * Performs authorization enforcement
   *
   * @param authorizationEnforcer the {@link AuthorizationEnforcer} to use for performing the enforcement
   * @param entities an {@link Object}[] of Strings from which an entity on which enforcement needs to be performed
   *                 can be created of just {@link EntityId} on which on whose parent enforcement needs to be performed
   * @param authenticationContext the {@link AuthenticationContext}  of the user that performs the action
   * @param actions the {@link Action}s to check for during enforcement
   * @throws Exception {@link UnauthorizedException} if the given authenticationContext is not authorized to perform
   * the specified actions on the entity
   */
  public static void enforce(AuthorizationEnforcer authorizationEnforcer, Object[] entities, Class enforceOn,
                             AuthenticationContext authenticationContext, Set<Action> actions) throws Exception {
    authorizationEnforcer.enforce(getEntityId(entities, enforceOn), authenticationContext.getPrincipal(), actions);
  }

  private static EntityId getEntityId(Object[] entities, Class enforceOn) {
    if (entities.length == 1 && entities[0] instanceof EntityId) {
      // If EntityId was passed in then the size of the array should be one
      return (EntityId) entities[0];
    } else {
      // If the size of the array is more than one we expect all the specified parameters to be String
      String[] entityParts;
      try {
        entityParts = Arrays.copyOf(entities, entities.length, String[].class);
      } catch (ArrayStoreException e) {
        // Arrays.copyOf will throw ArrayStoreException if an element copied from original array is not a String
        throw new IllegalArgumentException(String.format("%s annotation can be used for multiple part entities of " +
                                                                 "String type only.", AuthEnforce.class.getName()), e);
      }
      return EntityIdFactory.getEntityId(entityParts, enforceOn);
    }
  }

  private static class EntityIdFactory {
    public static EntityId getEntityId(String[] entityParts, Class enforceOn) {
      if (enforceOn.equals(InstanceId.class)) {
        return new InstanceId(entityParts[0]);
      } else if (enforceOn.equals(NamespaceId.class)) {
        return new NamespaceId(entityParts[0]);
      } else if (enforceOn.equals(ArtifactId.class)) {
        return new ArtifactId(entityParts[0], entityParts[1], entityParts[2]);
      } else if (enforceOn.equals(DatasetId.class)) {
        return new DatasetId(entityParts[0], entityParts[1]);
      } else if (enforceOn.equals(StreamId.class)) {
        return new StreamId(entityParts[0], entityParts[1]);
      } else if (enforceOn.equals(ApplicationId.class)) {
        return new ApplicationId(entityParts[0], entityParts[1]);
      } else if (enforceOn.equals(ProgramId.class)) {
        return new ProgramId(entityParts[0], entityParts[1], entityParts[2], entityParts[3]);
      } else {
        // safeguard: this should not happen since we have already verified all that the entity id can be created from
        // the given parts
        throw new IllegalArgumentException(String.format("Failed to create an %s from the the given entity parts %s",
                                                         enforceOn, Arrays.toString(entityParts)));
      }
    }
  }
}

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
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.objectweb.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

  private static final Map<Class<? extends EntityId>, Constructor<? extends EntityId>> CONS_CACHE =
    new ConcurrentHashMap<>();

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
  public static void enforce(AuthorizationEnforcer authorizationEnforcer, Object[] entities,
                             Class<? extends EntityId> entityClass, AuthenticationContext authenticationContext,
                             Set<Action> actions) throws Exception {
    authorizationEnforcer.enforce(getEntityId(entities, entityClass), authenticationContext.getPrincipal(), actions);
  }

  private static EntityId getEntityId(Object[] entities, Class<? extends EntityId> entityClass)
    throws IllegalAccessException, InstantiationException, InvocationTargetException {
    if (entities.length == 1 && entities[0] instanceof EntityId) {
      // If EntityId was passed in then the size of the array should be one
      // if entities length is 1 and the first element is an instance of EntityId then we know that the enforcement is
      // being done the specified entities itself. Check that the entity class (the one which was provided in the
      // enforceOn) is same and return the EntityId
      EntityId entityId = (EntityId) entities[0];
      if (entityId.getClass() == entityClass) {
        return entityId;
      }
      throw new IllegalArgumentException(String.format("Enforcement was specified on %s but an instance of %s was " +
                                                         "provided.", entityClass, entityId.getClass()));
    } else {
      return createEntityId(entityClass, entities);
    }
  }


  private static EntityId createEntityId(Class<? extends EntityId> entityClass, Object[] args)
    throws IllegalAccessException, InvocationTargetException, InstantiationException {
    Constructor<? extends EntityId> constructor = getConstructor(entityClass);

    // its okay to call with object [] without checking that all of these are string because if one of them is not
    // then newInstance call will throw IllegalArgumentException.
    return constructor.newInstance(args);
  }

  /**
   * Return the required size of entity parts to create the {@link EntityId} on which authorization enforcement
   * needs to be done as specified in {@link AuthEnforce#enforceOn()}
   *
   * @param enforceOn the {@link Type} of {@link EntityId} on which enforcement needs to be done
   * @return the size of entity parts needed to create the above {@link EntityId}
   * @throws IllegalArgumentException of the given enforceOn is not of supported {@link EntityId} type
   */
  static int verifyAndGetRequiredSize(Type enforceOn) {
    if (enforceOn.equals(Type.getType(InstanceId.class))) {
      return getConstructor(InstanceId.class).getParameterTypes().length;
    } else if (enforceOn.equals(Type.getType(NamespaceId.class))) {
      return getConstructor(NamespaceId.class).getParameterTypes().length;
    } else if (enforceOn.equals(Type.getType(StreamId.class))) {
      return getConstructor(StreamId.class).getParameterTypes().length;
    } else if (enforceOn.equals(Type.getType(DatasetId.class))) {
      return getConstructor(DatasetId.class).getParameterTypes().length;
    } else if (enforceOn.equals(Type.getType(ApplicationId.class))) {
      return getConstructor(ApplicationId.class).getParameterTypes().length;
    } else if (enforceOn.equals(Type.getType(ArtifactId.class))) {
      return getConstructor(ArtifactId.class).getParameterTypes().length;
    } else if (enforceOn.equals(Type.getType(ProgramId.class))) {
      return getConstructor(ProgramId.class).getParameterTypes().length;
    } else {
      throw new IllegalArgumentException(String.format("Failed to determine required number of entity parts " +
                                                         "needed for %s. Please make sure its a valid %s class " +
                                                         "for authorization enforcement",
                                                       enforceOn.getClassName(), EntityId.class.getSimpleName()));
    }
  }

  private static Constructor<? extends EntityId> getConstructor(Class<? extends EntityId> entityClass) {
    Constructor<? extends EntityId> constructor = CONS_CACHE.get(entityClass);
    if (constructor != null) {
      return constructor;
    }

    // Find the constructor with all String parameters
    for (Constructor<?> curConstructor : entityClass.getConstructors()) {
      if (Iterables.all(Arrays.asList(curConstructor.getParameterTypes()),
                        Predicates.<Class<?>>equalTo(String.class))) {
        constructor = (Constructor<? extends EntityId>) curConstructor;
        break;
      }
    }
    if (constructor != null) {
      // It's ok to just put. If there are concurrent calls, both of them should end up with the same constructor.
      CONS_CACHE.put(entityClass, constructor);
      return constructor;
    }
    // since constructor was not found throw an exception
    throw new IllegalStateException(String.format("Failed to find constructor for %s whose parameters are only of " +
                                                    "String type", entityClass.getName()));
  }
}

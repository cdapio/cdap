/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.common.security;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ParentedId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.security.spi.AccessIOException;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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

  private static final Map<Class<? extends EntityId>, Constructor<? extends EntityId>> CONS_CACHE;

  static {
    CONS_CACHE = new IdentityHashMap<>();
    CONS_CACHE.put(InstanceId.class, findConstructor(InstanceId.class));
    CONS_CACHE.put(NamespaceId.class, findConstructor(NamespaceId.class));
    CONS_CACHE.put(DatasetId.class, findConstructor(DatasetId.class));
    CONS_CACHE.put(ApplicationId.class, findConstructor(ApplicationId.class));
    CONS_CACHE.put(ArtifactId.class, findConstructor(ArtifactId.class));
    CONS_CACHE.put(ProgramId.class, findConstructor(ProgramId.class));
  }

  /**
   * Performs authorization enforcement
   *
   * @param accessEnforcer the {@link AccessEnforcer} to use for performing the enforcement
   * @param entities an {@link Object}[] of Strings from which an entity on which enforcement needs to be
   * performed
   * can be created of just {@link EntityId} on which on whose parent enforcement needs
   * to be performed
   * @param authenticationContext the {@link AuthenticationContext}  of the user that performs the action
   * @param permissions the {@link Permission}s to check for during enforcement
   * @throws Exception {@link UnauthorizedException} if the given authenticationContext is not authorized to perform
   * the specified permissions on the entity
   */
  public static void enforce(AccessEnforcer accessEnforcer, Object[] entities,
                             Class<? extends EntityId> entityClass, AuthenticationContext authenticationContext,
                             Set<? extends Permission> permissions) throws Exception {
    accessEnforcer.enforce(getEntityId(entities, entityClass), authenticationContext.getPrincipal(), permissions);
  }

  private static EntityId getEntityId(Object[] entities, Class<? extends EntityId> entityClass)
    throws IllegalAccessException, InstantiationException, InvocationTargetException {
    if (entities.length == 1 && entities[0] instanceof EntityId) {
      // If EntityId was passed in then the size of the array should be one
      // if entities length is 1 and the first element is an instance of EntityId then we know that the enforcement is
      // being done the specified entities itself. Check that the entity class (the one which was provided in the
      // enforceOn) is same as the entity of one of its parents and return the EntityId
      EntityId entityId = (EntityId) entities[0];
      if (entityId.getClass() == entityClass) {
        return entityId;
      }
      // Check if parent is one of the entity types being enforced.
      while (entityId instanceof ParentedId) {
        entityId = ((ParentedId) entityId).getParent();
        if (entityId != null && entityId.getClass() == entityClass) {
          return entityId;
        }
      }

      throw new IllegalArgumentException(String.format("Enforcement was specified on %s but an instance of %s was " +
                                                         "provided.", entityClass, entityId.getClass()));
    } else {
      return createEntityId(entityClass, entities);
    }
  }

  /**
   * Return the required size of entity parts to create the {@link EntityId} on which authorization enforcement
   * needs to be done as specified in {@link AuthEnforce#enforceOn()}
   *
   * @param enforceOn the {@link Type} of {@link EntityId} on which enforcement needs to be done
   * @return the size of entity parts needed to create the above {@link EntityId}
   * @throws IllegalArgumentException of the given enforceOn is not of supported {@link EntityId} type
   */
  static int getEntityIdPartsCount(Type enforceOn) {
    if (enforceOn.equals(Type.getType(InstanceId.class))) {
      return CONS_CACHE.get(InstanceId.class).getParameterTypes().length;
    }
    if (enforceOn.equals(Type.getType(NamespaceId.class))) {
      return CONS_CACHE.get(NamespaceId.class).getParameterTypes().length;
    }
    if (enforceOn.equals(Type.getType(DatasetId.class))) {
      return CONS_CACHE.get(DatasetId.class).getParameterTypes().length;
    }
    if (enforceOn.equals(Type.getType(ApplicationId.class))) {
      return CONS_CACHE.get(ApplicationId.class).getParameterTypes().length;
    }
    if (enforceOn.equals(Type.getType(ArtifactId.class))) {
      return CONS_CACHE.get(ArtifactId.class).getParameterTypes().length;
    }
    if (enforceOn.equals(Type.getType(ProgramId.class))) {
      return CONS_CACHE.get(ProgramId.class).getParameterTypes().length;
    }
    throw new IllegalArgumentException(String.format("Failed to determine required number of entity parts " +
                                                       "needed for %s. Please make sure its a valid %s class " +
                                                       "for authorization enforcement",
                                                     enforceOn.getClassName(), EntityId.class.getSimpleName()));
  }

  private static EntityId createEntityId(Class<? extends EntityId> entityClass, Object[] args)
    throws IllegalAccessException, InvocationTargetException, InstantiationException {
    Constructor<? extends EntityId> constructor = CONS_CACHE.get(entityClass);

    Preconditions.checkNotNull(constructor, String.format("Failed to find constructor for entity class %s. Please " +
                                                            "make sure it exists.", entityClass));
    // its okay to call with object [] without checking that all of these are string because if one of them is not
    // then newInstance call will throw IllegalArgumentException.
    return constructor.newInstance(args);
  }

  private static Constructor<? extends EntityId> findConstructor(Class<? extends EntityId> entityClass) {
    // Find the constructor with all String parameters
    for (Constructor<?> curConstructor : entityClass.getConstructors()) {
      if (Arrays.stream(curConstructor.getParameterTypes()).allMatch(String.class::equals)) {
        return (Constructor<? extends EntityId>) curConstructor;
      }
    }
    // since constructor was not found throw an exception
    throw new IllegalStateException(String.format("Failed to find constructor for %s whose parameters are only of " +
                                                    "String type", entityClass.getName()));
  }

  /**
   * Helper method for traversing a class's {@link ParentedId} tree using DFS.
   *
   * @param entityIdClass The entityId class to check
   * @param enforceOnClass The enforceOn class to check against
   * @return whether the entityIdClass or its parent classes are equal to the enforceOn class
   */
  public static boolean verifyEntityIdParents(Class entityIdClass, Class enforceOnClass) {
    if (entityIdClass.equals(enforceOnClass)) {
      return true;
    }
    if (!ParentedId.class.isAssignableFrom(entityIdClass)) {
      return false;
    }
    java.lang.reflect.Type[] implementedInterfaces = entityIdClass.getGenericInterfaces();
    for (java.lang.reflect.Type implementedInterface : implementedInterfaces) {
      if (implementedInterface instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) implementedInterface;
        if (parameterizedType.getRawType().getTypeName().equals(ParentedId.class.getCanonicalName())) {
          java.lang.reflect.Type[] parameterTypes = parameterizedType.getActualTypeArguments();
          if (parameterTypes.length != 1) {
            return false;
          }
          java.lang.reflect.Type parameter = parameterTypes[0];
          if (parameter instanceof Class && verifyEntityIdParents((Class) parameter, enforceOnClass)) {
            return true;
          }
        }
      }
    }
    // If no interfaces match, check superclass
    return verifyEntityIdParents(entityIdClass.getSuperclass(), enforceOnClass);
  }

  public static AccessException propagateAccessException(Throwable e) throws AccessException {
    if (e.getCause() != null && (e instanceof ExecutionException || e instanceof UncheckedExecutionException)) {
      propagateAccessException(e.getCause());
    }
    Throwables.propagateIfPossible(e, AccessException.class);
    if (e instanceof IOException) {
      return new AccessIOException(e);
    }
    return new AccessException(e);
  }
}

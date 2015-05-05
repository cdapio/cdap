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

package co.cask.cdap.common.lang;

import co.cask.cdap.internal.lang.Reflections;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * InstantiatorFactory.
 */
@SuppressWarnings("unchecked")
public final class InstantiatorFactory {

  private static final Unsafe UNSAFE;

  private final LoadingCache<TypeToken<?>, Instantiator<?>> instantiatorCache;

  static {
    Unsafe unsafe;
    try {
      Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
      Field f = unsafeClass.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      unsafe = (Unsafe) f.get(null);
    } catch (Exception e) {
      unsafe = null;
    }
    UNSAFE = unsafe;
  }

  public InstantiatorFactory(final boolean useKnownType) {
    instantiatorCache = CacheBuilder.newBuilder().build(new CacheLoader<TypeToken<?>, Instantiator<?>>() {
      @Override
      public Instantiator<?> load(TypeToken<?> type) throws Exception {
        Instantiator<?> creator = getByDefaultConstructor(type);
        if (creator != null) {
          return creator;
        }

        if (useKnownType) {
          creator = getByKnownType(type);
          if (creator != null) {
            return creator;
          }
        }

        return getByUnsafe(type);
      }
    });
  }

  public <T> Instantiator<T> get(TypeToken<T> type) {
    return (Instantiator<T>) instantiatorCache.getUnchecked(type);
  }

  /**
   * Returns an {@link Instantiator} that uses default constructor to instantiate an object of the given type.
   *
   * @param type
   * @param <T>
   * @return
   */
  private <T> Instantiator<T> getByDefaultConstructor(TypeToken<T> type) {
    try {
      final Constructor<? super T> defaultCons = type.getRawType().getDeclaredConstructor();
      defaultCons.setAccessible(true);

      return new Instantiator<T>() {
        @Override
        public T create() {
          try {
            return (T) defaultCons.newInstance();
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };

    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private <T> Instantiator<T> getByKnownType(TypeToken<T> type) {
    Class<? super T> rawType = type.getRawType();
    if (rawType.isArray()) {
      return new Instantiator<T>() {
        @Override
        public T create() {
          return (T) Lists.newLinkedList();
        }
      };
    }
    if (Collection.class.isAssignableFrom(rawType)) {
      if (SortedSet.class.isAssignableFrom(rawType)) {
        return new Instantiator<T>() {
          @Override
          public T create() {
            return (T) Sets.newTreeSet();
          }
        };
      }
      if (Set.class.isAssignableFrom(rawType)) {
        return new Instantiator<T>() {
          @Override
          public T create() {
            return (T) Sets.newHashSet();
          }
        };
      }
      if (Queue.class.isAssignableFrom(rawType)) {
        return new Instantiator<T>() {
          @Override
          public T create() {
            return (T) Lists.newLinkedList();
          }
        };
      }
      return new Instantiator<T>() {
        @Override
        public T create() {
          return (T) Lists.newArrayList();
        }
      };
    }

    if (Map.class.isAssignableFrom(rawType)) {
      if (SortedMap.class.isAssignableFrom(rawType)) {
        return new Instantiator<T>() {
          @Override
          public T create() {
            return (T) Maps.newTreeMap();
          }
        };
      }
      return new Instantiator<T>() {
        @Override
        public T create() {
          return (T) Maps.newHashMap();
        }
      };
    }
    return null;
  }

  private <T> Instantiator<T> getByUnsafe(final TypeToken<T> type) {
    return new Instantiator<T>() {
      @Override
      public T create() {
        try {
          Object instance = UNSAFE.allocateInstance(type.getRawType());
          Reflections.visit(instance, type, new FieldInitializer());
          return (T) instance;
        } catch (InstantiationException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}

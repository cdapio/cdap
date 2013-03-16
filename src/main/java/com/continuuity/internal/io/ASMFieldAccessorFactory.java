package com.continuuity.internal.io;

import com.continuuity.internal.asm.ByteCodeClassLoader;
import com.continuuity.internal.asm.ClassDefinition;
import com.continuuity.internal.reflect.Fields;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.Map;

/**
 *
 */
public final class ASMFieldAccessorFactory implements FieldAccessorFactory {

  private final LoadingCache<FieldEntry, FieldAccessor> fieldAccessorCache;

  public ASMFieldAccessorFactory() {
    this.fieldAccessorCache = CacheBuilder.newBuilder().build(new FieldAccessorLoader());
  }

  @Override
  public FieldAccessor getFieldAccessor(TypeToken<?> type, String fieldName) {
    return fieldAccessorCache.getUnchecked(new FieldEntry(type, fieldName));
  }

  private static final class FieldAccessorLoader extends CacheLoader<FieldEntry, FieldAccessor> {

    private final Map<ClassLoader, FieldAccessorClassLoader> classLoaders = Maps.newHashMap();

    @Override
    public FieldAccessor load(FieldEntry key) throws Exception {
      Method defineClass = null;
      try {
        defineClass = ClassLoader.class.getDeclaredMethod("defineClass", String.class,
                                                          byte[].class, int.class, int.class);
        defineClass.setAccessible(true);
      } catch (Exception e) {
        // ok to ignore this exception, it will resort to the slow reflection way.
      }

      ClassDefinition classDef = new FieldAccessorGenerator()
                                      .generate(key.getType(),
                                                Fields.findField(key.getType(), key.getFieldName()),
                                                defineClass == null);

      ClassLoader classLoader = key.getType().getClass().getClassLoader();
      FieldAccessorClassLoader fieldAccessorClassLoader = classLoaders.get(classLoader);
      if (fieldAccessorClassLoader == null) {
        fieldAccessorClassLoader = new FieldAccessorClassLoader(classLoader, defineClass);
        classLoaders.put(classLoader, fieldAccessorClassLoader);
      }
      return (FieldAccessor) fieldAccessorClassLoader.addClass(classDef).loadClass(classDef.getClassName())
                                                .getConstructor(TypeToken.class).newInstance(key.getType());
    }
  }

  private static final class FieldAccessorClassLoader extends ByteCodeClassLoader {

    private final Method defineClassMethod;

    private FieldAccessorClassLoader(ClassLoader parent, Method defineClassMethod) {
      super(parent);
      this.defineClassMethod = defineClassMethod;
    }

    @Override
    public synchronized Class<?> loadClass(String className, boolean resolveIt) throws ClassNotFoundException {
      Class<?> result = findLoadedClass(className);
      if (result != null) {
        return result;
      }

      byte[] bytecode = bytecodes.get(className);
      if (bytecode == null || defineClassMethod == null) {
        return super.loadClass(className, resolveIt);
      }
      try {
        Method findLoadedClass = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
        findLoadedClass.setAccessible(true);
        result = (Class<?>) findLoadedClass.invoke(getParent(), className);
        if (result != null) {
          return result;
        }

        // Try to load the class from the same classloader of the given type.
        return (Class<?>) defineClassMethod.invoke(getParent(), className, bytecode, 0, bytecode.length);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}

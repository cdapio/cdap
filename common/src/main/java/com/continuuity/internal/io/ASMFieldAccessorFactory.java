package com.continuuity.internal.io;

import com.continuuity.internal.asm.ByteCodeClassLoader;
import com.continuuity.internal.asm.ClassDefinition;
import com.continuuity.common.lang.Fields;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * A {@link FieldAccessorFactory} that uses ASM to generate a specific {@link FieldAccessor} class
 * for each field. The resulting {@link FieldAccessor} instance will be cached and reused.
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

  /**
   * The {@link CacheLoader} for generating instance of {@link FieldAccessor} instance.
   */
  private static final class FieldAccessorLoader extends CacheLoader<FieldEntry, FieldAccessor> {

    /**
     * Map from ClassLoader of the origin field class to the ClassLoader for loading the generating FieldAccessor class.
     */
    private final Map<ClassLoader, FieldAccessorClassLoader> classLoaders = Maps.newHashMap();

    @Override
    public FieldAccessor load(FieldEntry key) throws Exception {
      // See if are able to use the "defineClass" method in the ClassLoader of the field class.
      Method defineClass = null;
      try {
        defineClass = ClassLoader.class.getDeclaredMethod("defineClass", String.class,
                                                          byte[].class, int.class, int.class);
        defineClass.setAccessible(true);
      } catch (Exception e) {
        // ok to ignore this exception, it will resort to the slow reflection way.
      }

      // Generate the FieldAccessor class bytecode.
      ClassDefinition classDef = new FieldAccessorGenerator()
                                      .generate(key.getType(),
                                                Fields.findField(key.getType(), key.getFieldName()),
                                                defineClass == null);

      ClassLoader classLoader = key.getType().getRawType().getClassLoader();
      FieldAccessorClassLoader fieldAccessorClassLoader = classLoaders.get(classLoader);
      if (fieldAccessorClassLoader == null) {
        fieldAccessorClassLoader = new FieldAccessorClassLoader(classLoader, defineClass);
        classLoaders.put(classLoader, fieldAccessorClassLoader);
      }
      return (FieldAccessor) fieldAccessorClassLoader.addClass(classDef).loadClass(classDef.getClassName())
                                                .getConstructor(TypeToken.class).newInstance(key.getType());
    }
  }

  /**
   * The ClassLoader for loading the FieldAccessor class. It will try to define the class from the parent
   * ClassLoader.
   */
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

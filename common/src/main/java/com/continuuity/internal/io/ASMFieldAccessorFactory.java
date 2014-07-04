package com.continuuity.internal.io;

import com.continuuity.internal.asm.ClassDefinition;
import com.continuuity.internal.lang.Fields;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;

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

    // See if are able to use the "defineClass" method in the ClassLoader of the field class.
    private final Method defineClass;

    FieldAccessorLoader() {
      Method defineClass = null;
      try {
        defineClass = ClassLoader.class.getDeclaredMethod("defineClass", String.class,
                                                          byte[].class, int.class, int.class);
        defineClass.setAccessible(true);
      } catch (Exception e) {
        // ok to ignore this exception, it will resort to the slow reflection way.
      }
      this.defineClass = defineClass;
    }


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
      return createAccessor(key.getType(), classDef);
    }

    private FieldAccessor createAccessor(TypeToken<?> type, ClassDefinition classDef) throws Exception {
      // Must use the same classloader as the type.
      ClassLoader classLoader = type.getRawType().getClassLoader();
      String className = classDef.getClassName();

      Method findLoadedClass = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
      findLoadedClass.setAccessible(true);
      Class<?> result = (Class<?>) findLoadedClass.invoke(classLoader, className);
      if (result == null) {
        // Try to define the class from the same classloader of the given type.
        byte[] bytecode = classDef.getBytecode();
        result = (Class<?>) defineClass.invoke(classLoader, className, bytecode, 0, bytecode.length);
      }
      return (FieldAccessor) result.getConstructor(TypeToken.class).newInstance(type);
    }
  }
}

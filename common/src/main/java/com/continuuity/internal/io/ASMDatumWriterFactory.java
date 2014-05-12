package com.continuuity.internal.io;

import com.continuuity.common.lang.CombineClassLoader;
import com.continuuity.internal.asm.ByteCodeClassLoader;
import com.continuuity.internal.asm.ClassDefinition;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.inject.Inject;

/**
 * A factory class for creating {@link DatumWriter} instance for different data type and schema.
 * It serves as an in memory cache for generated {@link DatumWriter} {@link Class} using ASM.
 */
public final class ASMDatumWriterFactory implements DatumWriterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ASMDatumWriterFactory.class);

  private final LoadingCache<CacheKey, Class<DatumWriter<?>>> datumWriterClasses;
  private final FieldAccessorFactory fieldAccessorFactory;

  @Inject
  public ASMDatumWriterFactory(FieldAccessorFactory fieldAccessorFactory) {
    this.fieldAccessorFactory = fieldAccessorFactory;
    this.datumWriterClasses = CacheBuilder.newBuilder().build(new ASMCacheLoader());
  }

  /**
   * Creates a {@link DatumWriter} that is able to encode given data type with the given {@link Schema}.
   * The instance created is thread safe and reusable.
   *
   * @param type Type information of the data type to be encoded.
   * @param schema Schema of the data type.
   * @param <T> Type of the data type.
   * @return A {@link DatumWriter} instance.
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> DatumWriter<T> create(TypeToken<T> type, Schema schema) {
    try {
      Class<DatumWriter<?>> writerClass = datumWriterClasses.getUnchecked(new CacheKey(schema, type));
      return (DatumWriter<T>) writerClass.getConstructor(Schema.class, FieldAccessorFactory.class)
                                        .newInstance(schema, fieldAccessorFactory);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * A private {@link CacheLoader} for generating different {@link DatumWriter} {@link Class}.
   */
  private static final class ASMCacheLoader extends CacheLoader<CacheKey, Class<DatumWriter<?>>> {

    private final Map<TypeToken<?>, ByteCodeClassLoader> classloaders = Maps.newIdentityHashMap();

    @SuppressWarnings("unchecked")
    @Override
    public Class<DatumWriter<?>> load(CacheKey key) throws Exception {
      ClassDefinition classDef = new DatumWriterGenerator().generate(key.getType(), key.getSchema());

      ClassLoader typeClassloader = getClassLoader(key.getType());
      ByteCodeClassLoader classloader = classloaders.get(key.getType());
      if (classloader == null) {
        classloader = new ByteCodeClassLoader(typeClassloader);
        classloaders.put(key.getType(), classloader);
      }

      return (Class<DatumWriter<?>>) classloader.addClass(classDef).loadClass(classDef.getClassName());
    }
  }

  /**
   * Returns the ClassLoader of the given type. If the given type is a {@link ParameterizedType}, it returns
   * a {@link CombineClassLoader} of all types.
   *
   * @return A new CombineClassLoader. If no ClassLoader is found from the type,
   *         it returns the current thread context ClassLoader if it's not null, otherwise, return system ClassLoader.
   */
  private static ClassLoader getClassLoader(TypeToken<?> type) {
    Set<ClassLoader> classLoaders = Sets.newIdentityHashSet();

    // Breath first traversal into the Type.
    Queue<TypeToken<?>> queue = Lists.newLinkedList();
    queue.add(type);
    while (!queue.isEmpty()) {
      type = queue.remove();
      ClassLoader classLoader = type.getRawType().getClassLoader();
      if (classLoader != null) {
        classLoaders.add(classLoader);
      }

      if (type.getType() instanceof ParameterizedType) {
        for (Type typeArg : ((ParameterizedType) type.getType()).getActualTypeArguments()) {
          queue.add(TypeToken.of(typeArg));
        }
      }
    }

    // Optionally add the context ClassLoader
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader parent = (contextClassLoader == null) ? ClassLoader.getSystemClassLoader() : contextClassLoader;

    if (classLoaders.isEmpty()) {
      return parent;
    }
    if (classLoaders.size() == 1) {
      return classLoaders.iterator().next();
    }
    return new CombineClassLoader(parent, classLoaders);
  }

  private static final class CacheKey {
    private final Schema schema;
    private final TypeToken<?> type;

    private CacheKey(Schema schema, TypeToken<?> type) {
      this.schema = schema;
      this.type = type;
    }

    public Schema getSchema() {
      return schema;
    }

    public TypeToken<?> getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CacheKey cacheKey = (CacheKey) o;
      return schema.equals(cacheKey.schema) && type.equals(cacheKey.type);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(schema, type);
    }
  }
}

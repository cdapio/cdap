package com.continuuity.internal.app.runtime.flow;

import com.continuuity.internal.api.io.Schema;
import com.continuuity.internal.io.DatumWriter;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import org.objectweb.asm.Type;

/**
 * A factory class for creating {@link DatumWriter} instance for different data type and schema.
 * It serves as an in memory cache for generated {@link DatumWriter} {@link Class} using ASM.
 */
public final class ASMDatumWriterFactory {

  private final LoadingCache<CacheKey, Class<DatumWriter<?>>> datumWriterClasses;

  public ASMDatumWriterFactory() {
    datumWriterClasses = CacheBuilder.newBuilder().build(new ASMCacheLoader());
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
  public <T> DatumWriter<T> create(TypeToken<T> type, Schema schema) {
    try {
      Class<DatumWriter<?>> writerClass = datumWriterClasses.getUnchecked(new CacheKey(schema, type));
      return (DatumWriter<T>)writerClass.getConstructor(Schema.class).newInstance(schema);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * A private {@link CacheLoader} for generating different {@link DatumWriter} {@link Class}.
   */
  private static final class ASMCacheLoader extends CacheLoader<CacheKey, Class<DatumWriter<?>>> {

    private final ASMDatumWriterClassLoader classLoader = new ASMDatumWriterClassLoader(getClass().getClassLoader());

    @Override
    public Class<DatumWriter<?>> load(CacheKey key) throws Exception {
      DatumWriterGenerator.ClassDefinition classDef = new DatumWriterGenerator().generate(key.getType(),
                                                                                          key.getSchema());

      return (Class<DatumWriter<?>>) classLoader.defineClass(Type.getObjectType(classDef.getClassName()).getClassName(),
                                                             classDef.getBytecode());
    }
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

  /**
   * A private {@link ClassLoader} for loading generated {@link DatumWriter} bytecode.
   */
  private static final class ASMDatumWriterClassLoader extends ClassLoader {

    private ASMDatumWriterClassLoader(ClassLoader parent) {
      super(parent);
    }

    private Class<?> defineClass(String name, byte[] bytes) {
      return defineClass(name, bytes, 0, bytes.length);
    }
  }
}

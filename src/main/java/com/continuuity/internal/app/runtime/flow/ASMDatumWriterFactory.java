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
 *
 */
public final class ASMDatumWriterFactory {

  private final LoadingCache<CacheKey, Class<DatumWriter<?>>> factories;

  public ASMDatumWriterFactory() {
    factories = CacheBuilder.newBuilder().build(new ASMCacheLoader());
  }

  public <T> DatumWriter<T> create(TypeToken<T> type, Schema schema) {
    try {
      Class<DatumWriter<?>> writerClass = factories.getUnchecked(new CacheKey(schema, type));
      return (DatumWriter<T>)writerClass.getConstructor(Schema.class).newInstance(schema);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

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

  private static final class ASMDatumWriterClassLoader extends ClassLoader {

    private ASMDatumWriterClassLoader(ClassLoader parent) {
      super(parent);
    }

    private Class<?> defineClass(String name, byte[] bytes) {
      return defineClass(name, bytes, 0, bytes.length);
    }
  }
}

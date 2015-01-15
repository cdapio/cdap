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

package co.cask.cdap.internal.io;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.internal.asm.ByteCodeClassLoader;
import co.cask.cdap.internal.asm.ClassDefinition;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.util.Map;
import javax.inject.Inject;

/**
 * A factory class for creating {@link DatumWriter} instance for different data type and schema.
 * It serves as an in memory cache for generated {@link DatumWriter} {@link Class} using ASM.
 */
public final class ASMDatumWriterFactory implements DatumWriterFactory {

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

      ByteCodeClassLoader classloader = classloaders.get(key.getType());
      if (classloader == null) {
        // The ClassLoader of the generated DatumWriter has CDAP system ClassLoader as parent.
        // The ClassDefinition contains list of classes that should not be loaded by the generated class ClassLoader
        classloader = new ByteCodeClassLoader(ASMDatumWriterFactory.class.getClassLoader());
        classloaders.put(key.getType(), classloader);
      }

      return (Class<DatumWriter<?>>) classloader.addClass(classDef).loadClass(classDef.getClassName());
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
}

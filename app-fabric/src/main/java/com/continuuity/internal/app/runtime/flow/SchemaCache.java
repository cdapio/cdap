package com.continuuity.internal.app.runtime.flow;

import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaHash;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Dynamic loading of schema from classloader and caching of known schemas.
 */
public final class SchemaCache {

  private final LoadingCache<SchemaHash, Schema> cache;

  /**
   * Creates the schema cache with a set of know schemas.
   *
   * @param schemas Set of known schemas
   */
  public SchemaCache(Iterable<Schema> schemas, ClassLoader classLoader) {

    // TODO: Later on we should use ClassLoader.getResource
    final Map<SchemaHash, Schema> schemaMap = Maps.newHashMap();
    for (Schema schema : schemas) {
      schemaMap.put(schema.getSchemaHash(), schema);
    }

    cache = CacheBuilder.newBuilder().build(new CacheLoader<SchemaHash, Schema>() {
                                               @Override
                                               public Schema load(SchemaHash key) throws Exception {
                                                 Schema schema = schemaMap.get(key);
                                                 Preconditions.checkNotNull(schema);
                                                 // TODO: Load from classloader
                                                 return schema;
                                               }
                                             }
    );
  }

  /**
   * Reads a {@link SchemaHash} from the given buffer and returns a {@link Schema} associated
   * with the hash.
   *
   * @param buffer {@link ByteBuffer} for reading in schema hash
   * @return A {@link Schema} or {@code null} if the schema is not found.
   */
  public Schema get(ByteBuffer buffer) {
    return get(new SchemaHash(buffer));
  }

  public Schema get(SchemaHash hash) {
    try {
      return cache.get(hash);
    } catch (ExecutionException e) {
      return null;
    }
  }
}

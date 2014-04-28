package com.continuuity.metrics.data;

import com.continuuity.common.utils.ImmutablePair;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Class for encode/decode metric entities (context, metric and tag).
 */
final class MetricsEntityCodec {

  private static final long CACHE_EXPIRE_MINUTE = 10;
  private static final Pattern ENTITY_SPLITTER = Pattern.compile("\\.");
  private static final String[] EMPTY_STRINGS = new String[0];

  private final EntityTable entityTable;
  private final int contextDepth;
  private final int metricDepth;
  private final int tagDepth;
  private final EnumMap<MetricsEntityType, LoadingCache<String, byte[]>> entityCaches;


  MetricsEntityCodec(EntityTable entityTable, int contextDepth, int metricDepth, int tagDepth) {
    this.entityTable = entityTable;
    this.contextDepth = contextDepth;
    this.metricDepth = metricDepth;
    this.tagDepth = tagDepth;

    this.entityCaches = Maps.newEnumMap(MetricsEntityType.class);
    for (MetricsEntityType type : MetricsEntityType.values()) {
      LoadingCache<String, byte[]> cache = CacheBuilder.newBuilder()
                                                       .expireAfterAccess(CACHE_EXPIRE_MINUTE, TimeUnit.MINUTES)
                                                       .build(createCacheLoader(type));
      this.entityCaches.put(type, cache);
    }
  }

  /**
   * Encodes a '.' separated entity into bytes.
   * @param type Type of the entity.
   * @param entity Value of the entity.
   * @return byte[] representing the given entity.
   */
  public byte[] encode(MetricsEntityType type, String entity) {
    return entityCaches.get(type).getUnchecked(entity);
  }

  /**
   * Encodes a dot separated entity into bytes without padding.
   * @param type Type of the entity.
   * @param entity Value of the entity.
   * @return byte[] representing the given entity.
   */
  public byte[] encodeWithoutPadding(MetricsEntityType type, String entity) {
    int idSize = entityTable.getIdSize();
    String[] entityParts = entity == null ? EMPTY_STRINGS : ENTITY_SPLITTER.split(entity);
    byte[] result = new byte[entityParts.length * idSize];

    for (int i = 0; i < entityParts.length; i++) {
      idToBytes(entityTable.getId(type.getType() + i, entityParts[i]), idSize, result, i * idSize);
    }
    return result;
  }

  /**
   * Encodes a '.' separated entity into bytes. If the entity has less than the given parts or {@code null},
   * the remaining bytes would be padded by the given padding.
   * @param type Type of the entity.
   * @param entity Value of the entity.
   * @param padding Padding byte to apply for padding.
   * @return byte[] representing the given entity that may have padding at the end.
   */
  public byte[] paddedEncode(MetricsEntityType type, String entity, int padding) {
    int idSize = entityTable.getIdSize();
    int depth = getDepth(type);
    String[] entityParts = entity == null ? EMPTY_STRINGS : ENTITY_SPLITTER.split(entity, depth);
    byte[] result = new byte[depth * idSize];

    for (int i = 0; i < entityParts.length; i++) {
      if (entityParts[i].isEmpty()) {
        throw new IllegalArgumentException("found empty part in metrics entity " + entity);
      }
      idToBytes(entityTable.getId(type.getType() + i, entityParts[i]), idSize, result, i * idSize);
    }

    Arrays.fill(result, entityParts.length * idSize, depth * idSize, (byte) (padding & 0xff));
    return result;
  }

  /**
   * Encodes a '.' separated entity into bytes. If the entity has less than the given parts, the remaining bytes
   * would be padded by the given padding. Also return a fuzzy mask with byte value = 1 for the padded bytes and
   * 0 for non padded bytes.
   *
   * @param type Type of the entity.
   * @param entity Value of the entity.
   * @param padding Padding byte to apply for padding.
   * @return ImmutablePair with first byte[] representing the given entity that may have padding at the end and
   *         second byte[] as the fuzzy mask.
   */
  public ImmutablePair<byte[], byte[]> paddedFuzzyEncode(MetricsEntityType type, String entity, int padding) {
    int idSize = entityTable.getIdSize();
    int depth = getDepth(type);
    String[] entityParts = entity == null ? EMPTY_STRINGS : ENTITY_SPLITTER.split(entity, depth);
    byte[] result = new byte[depth * idSize];
    byte[] mask = new byte[depth * idSize];

    Arrays.fill(mask, (byte) 0);

    for (int i = 0; i < entityParts.length; i++) {
      idToBytes(entityTable.getId(type.getType() + i, entityParts[i]), idSize, result, i * idSize);
    }

    Arrays.fill(result, entityParts.length * idSize, depth * idSize, (byte) (padding & 0xff));
    Arrays.fill(mask, entityParts.length * idSize, depth * idSize, (byte) 1);

    return new ImmutablePair<byte[], byte[]>(result, mask);
  }

  public String decode(MetricsEntityType type, byte[] encoded) {
    return decode(type, encoded, 0);
  }

  /**
   * Decodes a byte[] into '.' separated entity string.
   */
  public String decode(MetricsEntityType type, byte[] encoded, int offset) {
    StringBuilder builder = new StringBuilder();
    int idSize = entityTable.getIdSize();
    int length = getDepth(type);
    Preconditions.checkArgument(length > 0, "Too few bytes to decode.");

    for (int i = 0; i < length; i++) {
      long id = decodeId(encoded, offset + i * idSize, idSize);
      if (id == 0) {
        // It's the padding byte, break the loop.
        break;
      }
      builder.append(entityTable.getName(id, type.getType() + i))
        .append('.');
    }

    builder.setLength(builder.length() - 1);
    return builder.toString();
  }

  /**
   * Returns the number of bytes that the given entity type would occupy.
   */
  public int getEncodedSize(MetricsEntityType type) {
    return getDepth(type) * entityTable.getIdSize();
  }

  /**
   * Creates a CacheLoader for entity name to encoded byte[].
   * @param type Type of the entity.
   */
  private CacheLoader<String, byte[]> createCacheLoader(final MetricsEntityType type) {
    return new CacheLoader<String, byte[]>() {
      @Override
      public byte[] load(String key) throws Exception {
        return paddedEncode(type, key, 0);
      }
    };
  }

  private int getDepth(MetricsEntityType type) {
    switch (type) {
      case CONTEXT:
        return contextDepth;
      case RUN:
        return 1;   // RunId doesn't have hierarchy
      case METRIC:
        return metricDepth;
      case TAG:
        return tagDepth;
    }
    throw new IllegalArgumentException("Unsupported entity type: " + type);
  }

  /**
   * Save a long id into the given byte array, assuming the given array is always big enough.
   */
  private void idToBytes(long id, int idSize, byte[] bytes, int offset) {
    while (idSize != 0) {
      idSize--;
      bytes[offset + idSize] = (byte) (id & 0xff);
      id >>= 8;
    }
  }

  private long decodeId(byte[] bytes, int offset, int idSize) {
    long id = 0;
    for (int i = 0; i < idSize; i++) {
      id |= (bytes[offset + i] & 0xff) << ((idSize - i - 1) * 8);
    }
    return id;
  }
}

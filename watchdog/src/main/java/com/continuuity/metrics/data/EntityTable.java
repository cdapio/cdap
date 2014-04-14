/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.OperationResult;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * This class handle assignment of unique ID to entity name, persisted by a OVCTable.
 *
 * Entity table is for storing mapping between name to unique ID. There are two set of rows in this table,
 * one is for generating the next sequence ID using increment, the other stores the actual mappings.
 *
 * <h5>Generator rows</h5>
 * Row key is formated as {@code [type].maxId} and there is only one column "maxId" which stores the
 * last ID being generated. Each time when a new ID is needed for a given type, and increment and get on the
 * corresponding row would be called.
 *
 * <h5>Entity mapping rows</h5>
 * Each entity would have two rows. One is keyed by {@code [type].[entityName]} and have one "id" column which
 * stores the unique ID. The other is a reverse map from {@code [type].id} to entity name in "name" column.
 */
public final class EntityTable {

  private static final Logger LOG = LoggerFactory.getLogger(EntityTable.class);

  private static final byte[] ID = Bytes.toBytes("id");
  private static final byte[] MAX_ID = Bytes.toBytes("maxId");
  private static final byte[] NAME = Bytes.toBytes("name");
  private static final byte[] DOT = { '.' };

  private final MetricsTable table;
  private final LoadingCache<EntityName, Long> entityCache;
  private final LoadingCache<EntityId, EntityName> idCache;
  private final long maxId;
  private final int size;


  /**
   * Creates an EntityTable with max id = 65535.
   *
   * See {@link #EntityTable(MetricsTable, long)}.
   */
  public EntityTable(MetricsTable table) {
    this(table, 0x10000);
  }

  /**
   * Creates an EntityTable backed by the given {@link MetricsTable}.
   *
   * @param table The storage table
   * @param maxId Maximum ID (exclusive) that can be generated.
   */
  public EntityTable(MetricsTable table, long maxId) {
    Preconditions.checkArgument(table != null, "Table cannot be null.");
    Preconditions.checkArgument(maxId > 0, "maxId must be > 0.");

    this.table = table;
    this.entityCache = CacheBuilder.newBuilder().build(createEntityCacheLoader());
    this.idCache = CacheBuilder.newBuilder().build(createIdCacheLoader());
    this.maxId = maxId;
    this.size = computeSize(maxId);
  }

  /**
   * Returns an unique id for the given name.
   * @param name The {@link EntityName} to lookup.
   * @return Unique ID, it is guaranteed to be smaller than the maxId passed in constructor.
   */
  public long getId(String type, String name) {
    return entityCache.getUnchecked(new EntityName(type, name)) % maxId;
  }

  /**
   * Returns the entity name for the given id and type.
   * @param id The id to lookup
   * @param type The type of the entity.
   * @return The entity name with the given id assigned to.
   * @throws IllegalArgumentException if the given ID does not map to any name.
   */
  public String getName(long id, String type) {
    try {
      return idCache.get(new EntityId(id, type)).getName();
    } catch (ExecutionException e) {
      throw new IllegalArgumentException(e.getCause());
    }
  }

  /**
   * Returns number of bytes for ID represented by this table.
   */
  public int getIdSize() {
    return size;
  }

  private CacheLoader<EntityName, Long> createEntityCacheLoader() {
    return new CacheLoader<EntityName, Long>() {
      @Override
      public Long load(EntityName key) throws Exception {
        byte[] rowKey = Bytes.toBytes(key.getType() + '.' + key.getName());

        OperationResult<byte[]> result = table.get(rowKey, ID);

        // Found, return it
        if (!result.isEmpty()) {
          return Bytes.toLong(result.getValue());
        }

        // Not found, generate a new ID
        byte[] maxIdRowKey = Bytes.toBytes(key.getType() + ".maxId");
        long newId = table.incrementAndGet(maxIdRowKey, MAX_ID, 1L);
        Preconditions.checkState(newId < maxId, "Maximum %s ID generated.", maxId);

        if (key.getName() == null || key.getName().isEmpty()) {
          LOG.warn("Adding mapping for " + (key.getName() == null ? "null" : "empty") + " name, " +
                     " with type " + key.getType() + ", new id is " + newId);
        }

        // Save the mapping
        if (table.swap(rowKey, ID, null, Bytes.toBytes(newId))) {
          // Save the reverse mapping from r.type.id => name as well
          rowKey = Bytes.concat(Bytes.toBytes(key.getType()), DOT, Bytes.toBytes(newId));

          // It is wrong to have forward mapping set when reverse mapping failed to set, always try to overwrite it.
          byte[] oldName = null;
          while (!table.swap(rowKey, NAME, oldName, Bytes.toBytes(key.getName()))) {
            result = table.get(rowKey, NAME);
            if (result.isEmpty()) {
              throw new IllegalStateException("Fail to set reverse mapping from id to name.");
            }
            oldName = result.getValue();
          }

          return newId;
        }

        // Get the value if CAS failed.
        result = table.get(rowKey, ID);

        if (result.isEmpty()) {
          throw new IllegalStateException("ID not found for " + key);
        }
        return Bytes.toLong(result.getValue());
      }
    };
  }

  private CacheLoader<EntityId, EntityName> createIdCacheLoader() {
    return new CacheLoader<EntityId, EntityName>() {
      @Override
      public EntityName load(EntityId key) throws Exception {
        // Lookup the reverse mapping
        byte[] rowKey = Bytes.concat(Bytes.toBytes(key.getType()), DOT, Bytes.toBytes(key.getId()));
        OperationResult<byte[]> result = table.get(rowKey, NAME);
        if (result.isEmpty()) {
          throw new IllegalArgumentException("Entity name not found for type " + key.getType() + ", id " + key.getId());
        }
        return new EntityName(key.getType(), Bytes.toString(result.getValue()));
      }
    };
  }

  private int computeSize(long maxId) {
    byte[] bytes = Bytes.toBytes(maxId - 1);
    int size = bytes.length;
    for (byte b : bytes) {
      if (b != 0) {
        return size;
      }
      size--;
    }
    return size;
  }

  /**
   * Package private class to represent an entity name, which compose of type and name.
   */
  private static final class EntityName {

    private final String type;
    private final String name;

    EntityName(String type, String name) {
      Preconditions.checkArgument(type != null, "Type cannot be null.");
      Preconditions.checkArgument(name != null, "Name cannot be null.");
      this.type = type;
      this.name = name;
    }

    String getType() {
      return type;
    }

    String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      EntityName other = (EntityName) o;
      return type.equals(other.type) && name.equals(other.name);
    }

    @Override
    public int hashCode() {
      int result = type.hashCode();
      result = 31 * result + name.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
                    .add("type", type)
                    .add("name", name)
                    .toString();
    }
  }

  /**
   * Private class to hold both entity ID and the type.
   */
  private static final class EntityId {

    private final long id;
    private final String type;

    EntityId(long id, String type) {
      Preconditions.checkArgument(type != null, "Type cannot be null.");
      this.id = id;
      this.type = type;
    }

    long getId() {
      return id;
    }

    String getType() {
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

      EntityId other = (EntityId) o;

      return id == other.id && type.equals(other.type);
    }

    @Override
    public int hashCode() {
      int result = (int) (id ^ (id >>> 32));
      result = 31 * result + type.hashCode();
      return result;
    }
  }
}

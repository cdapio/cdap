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

package co.cask.cdap.proto;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

/**
 * Schema and other extended information about a Hive table.
 */
public class TableInfo {
  // NOTE: other available info include: privileges

  @SerializedName("table_name")
  private final String tableName;

  @SerializedName("db_name")
  private final String dbName;

  private final String owner;

  // In milliseconds
  @SerializedName("creation_time")
  private final long creationTime;

  // In milliseconds
  @SerializedName("last_access_time")
  private final long lastAccessTime;

  private final int retention;

  @SerializedName("partitioned_keys")
  private final List<ColumnInfo> partitionKeys;

  private final Map<String, String> parameters;

  @SerializedName("table_type")
  private final String tableType;

  private final List<ColumnInfo> schema;

  private final String location;

  @SerializedName("input_format")
  private final String inputFormat;

  @SerializedName("output_format")
  private final String outputFormat;

  private final boolean compressed;

  @SerializedName("num_buckets")
  private final int numBuckets;

  private final String serde;

  @SerializedName("serde_parameters")
  private final Map<String, String> serdeParameters;

  @SerializedName("from_dataset")
  private final boolean isBackedByDataset;

  public TableInfo(String tableName, String dbName, String owner, long creationTime, long lastAccessTime,
                   int retention, List<ColumnInfo> partitionKeys, Map<String, String> parameters,
                   String tableType, List<ColumnInfo> schema, String location, String inputFormat,
                   String outputFormat, boolean compressed, int numBuckets, String serde,
                   Map<String, String> serdeParameters, boolean isBackedByDataset) {
    this.tableName = tableName;
    this.dbName = dbName;
    this.owner = owner;
    this.creationTime = creationTime;
    this.lastAccessTime = lastAccessTime;
    this.retention = retention;
    this.partitionKeys = partitionKeys;
    this.parameters = ImmutableMap.copyOf(parameters);
    this.tableType = tableType;
    this.schema = schema;
    this.location = location;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.compressed = compressed;
    this.numBuckets = numBuckets;
    this.serde = serde;
    this.serdeParameters = ImmutableMap.copyOf(serdeParameters);
    this.isBackedByDataset = isBackedByDataset;
  }

  public String getTableName() {
    return tableName;
  }

  public String getDbName() {
    return dbName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public String getLocation() {
    return location;
  }

  public List<ColumnInfo> getSchema() {
    return schema;
  }

  public List<ColumnInfo> getPartitionKeys() {
    return partitionKeys;
  }

  public boolean isBackedByDataset() {
    return isBackedByDataset;
  }

  public Map<String, String> getSerdeParameters() {
    return serdeParameters;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableInfo that = (TableInfo) o;

    return Objects.equal(this.tableName, that.tableName) &&
      Objects.equal(this.dbName, that.dbName) &&
      Objects.equal(this.owner, that.owner) &&
      Objects.equal(this.creationTime, that.creationTime) &&
      Objects.equal(this.lastAccessTime, that.lastAccessTime) &&
      Objects.equal(this.retention, that.retention) &&
      Objects.equal(this.partitionKeys, that.partitionKeys) &&
      Objects.equal(this.parameters, that.parameters) &&
      Objects.equal(this.tableType, that.tableType) &&
      Objects.equal(this.schema, that.schema) &&
      Objects.equal(this.location, that.location) &&
      Objects.equal(this.inputFormat, that.inputFormat) &&
      Objects.equal(this.outputFormat, that.outputFormat) &&
      Objects.equal(this.compressed, that.compressed) &&
      Objects.equal(this.numBuckets, that.numBuckets) &&
      Objects.equal(this.serde, that.serde) &&
      Objects.equal(this.serdeParameters, that.serdeParameters) &&
      Objects.equal(this.isBackedByDataset, that.isBackedByDataset);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, dbName, owner, creationTime, lastAccessTime, retention, partitionKeys,
                            parameters, tableType, schema, location, inputFormat, outputFormat, compressed,
                            numBuckets, serde, serdeParameters, isBackedByDataset);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("tableName", tableName)
      .add("dbName", dbName)
      .add("owner", owner)
      .add("creationTime", creationTime)
      .add("lastAccessTime", lastAccessTime)
      .add("retention", retention)
      .add("partitionKeys", partitionKeys)
      .add("parameters", parameters)
      .add("tableType", tableType)
      .add("schema", schema)
      .add("location", location)
      .add("inputFormat", inputFormat)
      .add("outputFormat", outputFormat)
      .add("compressed", compressed)
      .add("numBuckets", numBuckets)
      .add("serde", serde)
      .add("serdeParameters", serdeParameters)
      .add("isBackedByDataset", isBackedByDataset)
      .toString();
  }

  /**
   * Column information, containing name, type and comment.
   */
  public static final class ColumnInfo {
    private final String name;
    private final String type;
    private final String comment;

    public ColumnInfo(String name, String type, String comment) {
      this.name = name;
      this.type = type;
      this.comment = comment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ColumnInfo that = (ColumnInfo) o;

      return Objects.equal(this.name, that.name) &&
        Objects.equal(this.type, that.type) &&
        Objects.equal(this.comment, that.comment);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, type, comment);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("comment", comment)
        .toString();
    }
  }
}

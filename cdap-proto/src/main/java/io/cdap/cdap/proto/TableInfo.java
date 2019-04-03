/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    this.parameters = Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
    this.tableType = tableType;
    this.schema = schema;
    this.location = location;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.compressed = compressed;
    this.numBuckets = numBuckets;
    this.serde = serde;
    this.serdeParameters = Collections.unmodifiableMap(new LinkedHashMap<>(serdeParameters));
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

    return Objects.equals(this.tableName, that.tableName) &&
      Objects.equals(this.dbName, that.dbName) &&
      Objects.equals(this.owner, that.owner) &&
      Objects.equals(this.creationTime, that.creationTime) &&
      Objects.equals(this.lastAccessTime, that.lastAccessTime) &&
      Objects.equals(this.retention, that.retention) &&
      Objects.equals(this.partitionKeys, that.partitionKeys) &&
      Objects.equals(this.parameters, that.parameters) &&
      Objects.equals(this.tableType, that.tableType) &&
      Objects.equals(this.schema, that.schema) &&
      Objects.equals(this.location, that.location) &&
      Objects.equals(this.inputFormat, that.inputFormat) &&
      Objects.equals(this.outputFormat, that.outputFormat) &&
      Objects.equals(this.compressed, that.compressed) &&
      Objects.equals(this.numBuckets, that.numBuckets) &&
      Objects.equals(this.serde, that.serde) &&
      Objects.equals(this.serdeParameters, that.serdeParameters) &&
      Objects.equals(this.isBackedByDataset, that.isBackedByDataset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, dbName, owner, creationTime, lastAccessTime, retention, partitionKeys,
                        parameters, tableType, schema, location, inputFormat, outputFormat, compressed,
                        numBuckets, serde, serdeParameters, isBackedByDataset);
  }

  @Override
  public String toString() {
    return "TableInfo{" +
      "tableName='" + tableName + '\'' +
      ", dbName='" + dbName + '\'' +
      ", owner='" + owner + '\'' +
      ", creationTime=" + creationTime +
      ", lastAccessTime=" + lastAccessTime +
      ", retention=" + retention +
      ", partitionKeys=" + partitionKeys +
      ", parameters=" + parameters +
      ", tableType='" + tableType + '\'' +
      ", schema=" + schema +
      ", location='" + location + '\'' +
      ", inputFormat='" + inputFormat + '\'' +
      ", outputFormat='" + outputFormat + '\'' +
      ", compressed=" + compressed +
      ", numBuckets=" + numBuckets +
      ", serde='" + serde + '\'' +
      ", serdeParameters=" + serdeParameters +
      ", isBackedByDataset=" + isBackedByDataset +
      '}';
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

      return Objects.equals(this.name, that.name) &&
        Objects.equals(this.type, that.type) &&
        Objects.equals(this.comment, that.comment);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type, comment);
    }

    @Override
    public String toString() {
      return "ColumnInfo{" +
        "name='" + name + '\'' +
        ", type='" + type + '\'' +
        ", comment='" + comment + '\'' +
        '}';
    }
  }
}

/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.api.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Entity representation for Metadata. This representation support representing any of the existing
 * CDAP entities which support metadata or any custom CDAP resource/entity. MetadataEntity are just ordered
 * <p>
 * key-value pair which represent the resource/entity.
 * <p>
 * Example usage:
 * Creating a MetadataEntity for stream
 * <pre>
 *  MetadataEntity metadataEntity = MetadataEntity.ofNamespace("myNamespace").append(MetadataEntity.STREAM, "myStream");
 * </pre>
 * <p>
 * Creating a Metadata for a field in dataset which is a custom resource
 * <pre>
 * MetadataEntity metadataEntity = MetadataEntity.ofNamespace("myNamespace")
 *   .append(MetadataEntity.DATASET, "myDataset").append("field", "myField");
 * </pre>
 * <p>
 * By default the key of the last key-value pair becomes the type of the MetadataEntity but it is also possible to
 * change the type on demand to represent the MetadataEntity to represent entities/resources whose type are not the key
 * of last key value pair. An example of this is ApplicationId which ends with "version" although the type is
 * application.
 * <pre>
 * MetadataEntity metadataEntity = MetadataEntity.ofNamespace("myNamespace")
 *   .append(MetadataEntity.APPLICATION, "myApplication")
 *   .append(MetadataEntity.VERSION, "appVersion");
 * metadataEntity = metadataEntity.changeType(MetadataEntity.APPLICATION);
 * </pre>
 */
public class MetadataEntity implements Iterable<MetadataEntity.KeyValue> {

  public static final String NAMESPACE = "namespace";
  public static final String APPLICATION = "application";
  public static final String ARTIFACT = "artifact";
  public static final String VERSION = "version";
  public static final String DATASET = "dataset";
  public static final String STREAM = "stream";
  public static final String VIEW = "stream_view";
  public static final String TYPE = "type";
  public static final String FLOW = "flow";
  public static final String FLOWLET = "flowlet";
  public static final String PROGRAM = "program";
  public static final String SCHEDULE = "schedule";
  public static final String PROGRAM_RUN = "program_run";

  private final LinkedHashMap<String, String> details;
  private String type;

  public MetadataEntity() {
    this.details = new LinkedHashMap<>();
  }

  private MetadataEntity(MetadataEntity metadataEntity) {
    this.details = new LinkedHashMap<>(metadataEntity.details);
    this.type = metadataEntity.type;
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName. To create a {@link MetadataEntity} for a
   * dataset in a specified namespace please use {@link MetadataEntity#ofDataset(String, String)}.
   *
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   */
  public static MetadataEntity ofDataset(String datasetName) {
    MetadataEntity metadataEntity = new MetadataEntity();
    metadataEntity.details.put(MetadataEntity.DATASET, datasetName);
    metadataEntity.type = MetadataEntity.DATASET;
    return metadataEntity;
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName in the specified namespace.
   *
   * @param namespace   the name of the namespace
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   */
  public static MetadataEntity ofDataset(String namespace, String datasetName) {
    MetadataEntity metadataEntity = new MetadataEntity();
    metadataEntity.details.put(MetadataEntity.NAMESPACE, namespace);
    metadataEntity.details.put(MetadataEntity.DATASET, datasetName);
    metadataEntity.type = MetadataEntity.DATASET;
    return metadataEntity;
  }

  /**
   * Creates a {@link MetadataEntity} representing the given namespace.
   *
   * @param namespace the name of the namespace
   * @return {@link MetadataEntity} representing the namespace name
   */
  public static MetadataEntity ofNamespace(String namespace) {
    MetadataEntity metadataEntity = new MetadataEntity();
    metadataEntity.details.put(MetadataEntity.NAMESPACE, namespace);
    metadataEntity.type = MetadataEntity.NAMESPACE;
    return metadataEntity;
  }

  /**
   * Creates a new {@link MetadataEntity} which consists of the given key and values following the key and values of
   * this {@link MetadataEntity} and is of type of the given key.
   *
   * @param key   the key to be added
   * @param value the value to be added
   * @return a new {@link MetadataEntity} which consists of the given key and values following the key and values of
   * this {@link MetadataEntity}
   */
  public MetadataEntity append(String key, String value) {
    MetadataEntity metadataEntity = new MetadataEntity(this);
    metadataEntity.details.put(key, value);
    metadataEntity.type = key;
    return metadataEntity;
  }

  /**
   * Creates a new MetadataEntity with the given type and existing {@link KeyValue} pairs
   *
   * @param newType the new type
   * @return the MetadataEntity which is of the specified type
   */
  public MetadataEntity changeType(String newType) {
    if (newType == null || newType.isEmpty()) {
      throw new IllegalArgumentException("A valid type must be specified.");
    }
    MetadataEntity metadataEntity = new MetadataEntity(this);
    metadataEntity.type = newType;
    return metadataEntity;
  }

  /**
   * @return the type of the MetadataEntity
   */
  public String getType() {
    return type;
  }

  /**
   * @return the value for the given key; if the key does not exists returns null;
   */
  @Nullable
  public String getValue(String key) {
    return details.get(key);
  }

  /**
   * @return true if there is an entry for the key in the MetadataEntity else false
   */
  public boolean containsKey(String key) {
    return details.containsKey(key);
  }

  /**
   * @return all the values in the MetadataEntity
   */
  public Iterable<String> getValues() {
    return Collections.unmodifiableList(new ArrayList<>(details.values()));
  }

  /**
   * @return all the keys in the MetadataEntity
   */
  public Iterable<String> getKeys() {
    return Collections.unmodifiableList(new ArrayList<>(details.keySet()));
  }

  /**
   * @return A {@link List} of {@link KeyValue} representing the metadata entity
   */
  @Override
  public Iterator<KeyValue> iterator() {
    List<KeyValue> result = new LinkedList<>();
    details.forEach((key, value) -> result.add(new KeyValue(key, value)));
    return result.stream().iterator();
  }

  @Override
  public String toString() {
    return "MetadataEntity{" +
      "details=" + details +
      ", type='" + type + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataEntity that = (MetadataEntity) o;
    return Objects.equals(details, that.details) &&
      Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(details, type);
  }

  /**
   * {@link MetadataEntity} key-value.
   */
  public static class KeyValue {
    private final String key;
    private final String value;

    KeyValue(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }
  }
}

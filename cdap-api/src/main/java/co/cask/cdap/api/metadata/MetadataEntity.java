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
 * <p>
 * Represents either a CDAP entity or a custom entity in metadata APIs. A {@code MetadataEntity} is an ordered
 * sequence of key/value pairs.
 * </p>
 * <p>
 * Example usage:
 * Creating a MetadataEntity for Dataset
 * </p>
 * <pre>
 *  MetadataEntity metadataEntity =
 *    MetadataEntity.ofNamespace("myNamespace").appendAsType(MetadataEntity.DATASET, "myDataset");
 * </pre>
 * <p>
 * Creating a {@code MetadataEntity} for a custom entity: a field in a dataset:
 * </p>
 * <pre>
 * MetadataEntity metadataEntity = MetadataEntity.ofNamespace("myNamespace")
 *   .append(MetadataEntity.DATASET, "myDataset").appendAsType("field", "myField");
 * </pre>
 * <p>
 * Every metadata entity has a type. The type can be changed during append by using
 * {@link #appendAsType(String, String)} which does not necessarily have to be the last append.
 * In some cases, the type is a key in middle. An example of this is ApplicationId which ends with "version"
 * although the type is "application".
 * </p>
 * <pre>
 * MetadataEntity metadataEntity = MetadataEntity.ofNamespace("myNamespace")
 *   .append(MetadataEntity.APPLICATION, "myApplication")
 *   .appendAsType(MetadataEntity.VERSION, "appVersion");
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

  /**
   * Creates a {@link MetadataEntity} with the given key and value and sets the type to the given key
   * @param key the key which will also be set as the type of the {@link MetadataEntity}
   * @param value the value
   */
  public MetadataEntity(String key, String value) {
    this.details = new LinkedHashMap<>();
    this.details.put(key, value);
    this.type = key;
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
    return new MetadataEntity(DATASET, datasetName);
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName in the specified namespace.
   *
   * @param namespace the name of the namespace
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   */
  public static MetadataEntity ofDataset(String namespace, String datasetName) {
    return MetadataEntity.ofNamespace(namespace).appendAsType(MetadataEntity.DATASET, datasetName);
  }

  /**
   * Creates a {@link MetadataEntity} representing the given namespace.
   *
   * @param namespace the name of the namespace
   * @return {@link MetadataEntity} representing the namespace name
   */
  public static MetadataEntity ofNamespace(String namespace) {
    return new MetadataEntity(NAMESPACE, namespace);
  }

  /**
   * Returns a new {@link MetadataEntity} which consists of the given key and values following the key and values of
   * this {@link MetadataEntity}. The returned {@link MetadataEntity} is of the same type. If the type needs to
   * changed during append then {@link #appendAsType(String, String)}  should be used.
   *
   * @param key the key to be added
   * @param value the value to be added
   * @return a new {@link MetadataEntity} which is of same type of this {@link MetadataEntity} but consists of
   * the given key and value following the key and values of this {@link MetadataEntity}
   */
  public MetadataEntity append(String key, String value) {
    MetadataEntity metadataEntity = new MetadataEntity(this);
    metadataEntity.details.put(key, value);
    return metadataEntity;
  }

  /**
   * Returns a new {@link MetadataEntity} which consists of the given key and values following the key and values of
   * this {@link MetadataEntity}. The returned {@link MetadataEntity} type is set to the given key. If an append is
   * required without the type change then {@link #append(String, String)} should be used.
   *
   * @param key the key to be added
   * @param value the value to be added
   * @return a new {@link MetadataEntity} whose type is set to the given key and consists of the given key and value
   * following the key and values of this {@link MetadataEntity}
   */
  public MetadataEntity appendAsType(String key, String value) {
    MetadataEntity metadataEntity = new MetadataEntity(this);
    metadataEntity.details.put(key, value);
    metadataEntity.type = key;
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

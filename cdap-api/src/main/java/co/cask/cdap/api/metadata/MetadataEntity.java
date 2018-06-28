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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * <p>
 * Represents either a CDAP entity or a custom entity in metadata APIs. A {@code MetadataEntity} is an ordered
 * sequence of key/value pairs constructed using {@link MetadataEntity.Builder}. Keys are case insensitive.
 * </p>
 * <p>
 * Example usage:
 * Creating a MetadataEntity for Dataset
 * </p>
 * <pre>
 *   MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
 *     .appendAsType(MetadataEntity.DATASET, "myDataset").build();
 * </pre>
 * <p>
 * Creating a {@code MetadataEntity} for a custom entity: a field in a dataset:
 * </p>
 * <pre>
 *   MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns").append(MetadataEntity.DATASET, "ds")
 *    .appendAsType("field", "myField").build();
 * </pre>
 * <p>
 * Every metadata entity has a type. If a type is not specifically specified by calling
 * {@link MetadataEntity.Builder#appendAsType(String, String)} the last key in the hierarchy will be the type by
 * default. In some cases, the type is a key in middle and {@link MetadataEntity.Builder#appendAsType(String, String)}
 * helps in representing these. An example of this is ApplicationId which ends with "version"
 * although the type is "application".
 * </p>
 * <pre>
 *   MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
 *     .appendAsType(MetadataEntity.APPLICATION, "myApp").append(MetadataEntity.VERSION, "1").build();
 * </pre>
 * This class also provide helper methods for creating {@link MetadataEntity} for:
 * <ul>
 * <li>CDAP Namespace: {@link #ofNamespace(String)}</li>
 * <li>CDAP Dataset: {@link #ofDataset(String, String)} and {@link #ofDataset(String)}.
 * The {@link #ofDataset(String)} lacks namespace information and does not represent complete information to
 * represent a Dataset. It is provided to conveniently refer to a Dataset in current namespace and should only be
 * used when it is known that the handler can fill in the namespace information.</li>
 * </ul>
 */
public class MetadataEntity implements Iterable<MetadataEntity.KeyValue> {

  public static final String NAMESPACE = "namespace";
  public static final String APPLICATION = "application";
  public static final String ARTIFACT = "artifact";
  public static final String VERSION = "version";
  public static final String DATASET = "dataset";
  public static final String V1_DATASET_INSTANCE = "datasetinstance";
  public static final String STREAM = "stream";
  public static final String VIEW = "stream_view";
  public static final String V1_VIEW = "view";
  public static final String TYPE = "type";
  public static final String FLOW = "flow";
  public static final String FLOWLET = "flowlet";
  public static final String PROGRAM = "program";
  public static final String SCHEDULE = "schedule";
  public static final String PROGRAM_RUN = "program_run";

  private final LinkedHashMap<String, String> details;
  private String type;

  private static final Map<String, String[][]> TYPES_TO_KEY_SEQUENCES;

  static {
    Map<String, String[][]> typesToKeys = new HashMap<>();
    typesToKeys.put(NAMESPACE, new String[][]{{NAMESPACE}});
    typesToKeys.put(DATASET, new String[][]{{NAMESPACE, DATASET}, {DATASET}});
    typesToKeys.put(STREAM, new String[][]{{NAMESPACE, STREAM}});
    typesToKeys.put(APPLICATION, new String[][]{{NAMESPACE, APPLICATION, VERSION}, {NAMESPACE, APPLICATION}});
    typesToKeys.put(ARTIFACT, new String[][]{{NAMESPACE, ARTIFACT, VERSION}});
    typesToKeys.put(VIEW, new String[][]{{NAMESPACE, STREAM, VIEW}});
    typesToKeys.put(PROGRAM, new String[][]{{NAMESPACE, APPLICATION, VERSION, TYPE, PROGRAM},
      {NAMESPACE, APPLICATION, TYPE, PROGRAM}});
    typesToKeys.put(SCHEDULE, new String[][]{{NAMESPACE, APPLICATION, VERSION, SCHEDULE},
      {NAMESPACE, APPLICATION, SCHEDULE}});
    typesToKeys.put(FLOWLET, new String[][]{{NAMESPACE, APPLICATION, VERSION, FLOW, FLOWLET},
      {NAMESPACE, APPLICATION, FLOW, FLOWLET}});
    typesToKeys.put(PROGRAM_RUN, new String[][]{{NAMESPACE, APPLICATION, VERSION, TYPE, PROGRAM, PROGRAM_RUN},
      {NAMESPACE, APPLICATION, TYPE, PROGRAM, PROGRAM_RUN}});
    TYPES_TO_KEY_SEQUENCES = Collections.unmodifiableMap(typesToKeys);
  }

  /**
   * Used by Builder to build the {@link MetadataEntity}
   */
  private MetadataEntity(LinkedHashMap<String, String> details, String type) {
    this.details = details;
    this.type = type;
  }

  /**
   * Builder for {@link MetadataEntity}
   */
  public static class Builder {
    private final LinkedHashMap<String, String> parts;
    private String type;

    Builder() {
      parts = new LinkedHashMap<>();
    }

    private Builder(MetadataEntity metadataEntity) {
      this.parts = new LinkedHashMap<>(metadataEntity.details);
      this.type = metadataEntity.type;
    }

    /**
     * Put the given key (case insensitive) with the given value in {@link MetadataEntity.Builder}.
     * The returned {@link MetadataEntity.Builder} is of the same type. If the type needs to
     * changed during append then {@link #appendAsType(String, String)}  should be used.
     *
     * @param key the key (case insensitive) to be added
     * @param value the value to be added
     * @return a new {@link MetadataEntity.Builder} which is of same type of this {@link MetadataEntity.Builder}
     * but consists of the given key and value in addition
     */
    public Builder append(String key, String value) {
      validateKey(key);
      parts.put(key.toLowerCase(), value);
      return this;
    }

    /**
     * Put the given key (case insensitive) with the given value in {@link MetadataEntity.Builder}.
     * The returned {@link MetadataEntity} type is set to the given key. If an append is required without the
     * type change then {@link #append(String, String)} should be used.
     *
     * @param key the key (case insensitive) to be added
     * @param value the value to be added
     * @return a new {@link MetadataEntity.Builder} whose type is set to the given key and consists of the given
     * key and value in addition
     */
    public Builder appendAsType(String key, String value) {
      validateKey(key);
      parts.put(key.toLowerCase(), value);
      this.type = key.toLowerCase();
      return this;
    }

    /**
     * Builds a {@link MetadataEntity} from the builder.
     *
     * @return {@link MetadataEntity} from the builder
     * @throws IllegalArgumentException if the key is a CDAP entity and the MetadataEntity is not correct to represent
     * the CDAP entity
     */
    public MetadataEntity build() {
      if (parts.isEmpty()) {
        throw new IllegalArgumentException("key-value pair must be specified");
      }
      if (type == null) {
        // traverse till the last key and make that the type
        parts.keySet().forEach(x -> type = x);
      }
      validateHierarchy();
      return new MetadataEntity(new LinkedHashMap<>(parts), type);
    }

    private void validateKey(String key) {
      if (key == null || key.isEmpty()) {
        throw new IllegalArgumentException("Key cannot be null or empty");
      }
      if (parts.containsKey(key)) {
        throw new IllegalArgumentException(String.format("key '%s' already exists in '%s'", key, parts));
      }
    }

    private void validateHierarchy() {
      if (TYPES_TO_KEY_SEQUENCES.containsKey(type)) {
        String[][] validSequences = TYPES_TO_KEY_SEQUENCES.get(type);
        for (String[] validSequence : validSequences) {
          if (Arrays.equals(validSequence, parts.keySet().toArray())) {
            // valid sequence found
            return;
          }
        }
        throw new IllegalArgumentException(String.format("Failed to build MetadataEntity of type '%s' from '%s'. " +
                                                           "Type '%s is a CDAP entity type and must follow one of " +
                                                           "the following key hierarchies '%s'." +
                                                           "If you want to represent a CDAP Entity please follow the " +
                                                           "correct hierarchy. If you are trying to represent a " +
                                                           "custom resource please use a different type name.",
                                                         type, parts, type, Arrays.toString(validSequences)));
      }
    }
  }

  public MetadataEntity(MetadataEntity metadataEntity) {
    this.details = metadataEntity.details;
    this.type = metadataEntity.type;
  }

  /**
   * Creates a {@link MetadataEntity} representing the given namespace.
   *
   * @param namespace the name of the namespace
   * @return {@link MetadataEntity} representing the namespace name
   * @throws IllegalArgumentException if the key is a CDAP entity and the MetadataEntity is not correct to represent
   * the CDAP entity
   */
  public static MetadataEntity ofNamespace(String namespace) {
    return builder().appendAsType(MetadataEntity.NAMESPACE, namespace).build();
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName. To create a {@link MetadataEntity} for a
   * dataset in a specified namespace please use {@link MetadataEntity#ofDataset(String, String)}.
   *
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   */
  public static MetadataEntity ofDataset(String datasetName) {
    return builder().appendAsType(MetadataEntity.DATASET, datasetName).build();
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName in the specified namespace.
   *
   * @param namespace the name of the namespace
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   * @throws IllegalArgumentException if the key is a CDAP entity and the MetadataEntity is not correct to represent
   * the CDAP entity
   */
  public static MetadataEntity ofDataset(String namespace, String datasetName) {
    return builder().append(MetadataEntity.NAMESPACE, namespace)
      .appendAsType(MetadataEntity.DATASET, datasetName).build();
  }

  /**
   * Returns a List of {@link KeyValue} till the given splitKey (inclusive)
   *
   * @param splitKey the splitKey (inclusive)
   * @return List of {@link KeyValue}
   */
  public List<MetadataEntity.KeyValue> head(String splitKey) {
    splitKey = splitKey.toLowerCase();
    if (!containsKey(splitKey)) {
      throw new IllegalArgumentException(String.format("The given key %s does not exists in %s", splitKey, toString()));
    }
    List<MetadataEntity.KeyValue> subParts = new ArrayList<>();
    for (KeyValue keyValue : this) {
      subParts.add(keyValue);
      if (keyValue.getKey().equalsIgnoreCase(splitKey)) {
        // reached till the key which is inclusive so stop
        break;
      }
    }
    return subParts;
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
  Iterable<String> getValues() {
    return details.values();
  }

  /**
   * @return all the keys in the MetadataEntity
   */
  public Iterable<String> getKeys() {
    return details.keySet();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(MetadataEntity metadataEntity) {
    return new Builder(metadataEntity);
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

    public KeyValue(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyValue keyValue = (KeyValue) o;
      return Objects.equals(key, keyValue.key) &&
        Objects.equals(value, keyValue.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }
  }
}

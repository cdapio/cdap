/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.api.dataset.lib.cask360;

import co.cask.cdap.api.dataset.lib.cask360.Cask360Group.Cask360GroupType;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataMap;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a single entity in a {@link Cask360Table}.
 * <p>
 * An entity is represented by a unique String id and any number of groups of
 * data about the entity. Each group of data is identified by a String name and
 * a {@link Cask360Group} object.
 */
public class Cask360Entity implements Comparable<Cask360Entity> {

  /** Gson with custom serialization */
  private static final Gson gson = Cask360Entity.getGson();

  /** Entity ID */
  private String id;

  /** Map from group name to group data */
  Map<String, Cask360Group> groups;

  /**
   * Constructs an empty entity with the specified ID.
   *
   * @param id
   *          unique identifier of the entity
   */
  public Cask360Entity(String id) {
    this(id, new TreeMap<String, Cask360Group>());
  }

  /**
   * Constructs a new instance of a {@link Cask360Entity}. References the
   * specified map, does not create an internal copy.
   *
   * @param id
   *          the id of the entity
   * @param groups
   *          groups of data of the entity
   */
  public Cask360Entity(String id, Map<String, Cask360Group> groups) {
    this.id = id;
    this.groups = groups;
  }

  /**
   * Get the ID of the entity.
   *
   * @return unique identifier string of this entity
   */
  public String getID() {
    return id;
  }

  /**
   * Get the groups of data of the entity.
   *
   * @return map of group names to group data
   */
  public Map<String, Cask360Group> getGroups() {
    return groups;
  }

  // Write Operations

  /**
   * Write the specified data key and value to the group with the specified
   * name.
   * <p>
   * Assumes the group is of the type {@link Cask360GroupType#MAP}, otherwise
   * will throw an exception.
   *
   * @param name
   *          the group name
   * @param key
   *          the data key
   * @param value
   *          the data value
   */
  public void write(String name, String key, String value) {
    Cask360Group group = groups.get(name);
    if (group != null) {
      group.getDataAsMap().put(key, value);
    } else {
      Cask360GroupDataMap dataMap = new Cask360GroupDataMap();
      dataMap.put(key, value);
      group = new Cask360Group(name, dataMap);
      groups.put(name, group);
    }
  }

  /**
   * Write the specified data map to the group with the specified name.
   * <p>
   * Assumes the group is of the type {@link Cask360GroupType#MAP}, otherwise
   * will throw an exception.
   *
   * @param name
   *          the group name
   * @param data
   *          the data
   */
  public void write(String name, Map<String, String> data) {
    Cask360Group group = groups.get(name);
    if (group != null) {
      group.getDataAsMap().putAll(data);
    } else {
      Cask360GroupDataMap dataMap = new Cask360GroupDataMap();
      dataMap.putAll(data);
      group = new Cask360Group(name, dataMap);
      groups.put(name, group);
    }
  }

  /**
   * Write the specified data time and value to the group with the specified
   * name.
   * <p>
   * Overwrites all existing data (all keys/values) at this time for this group.
   * <p>
   * Assumes the group is of the type {@link Cask360GroupType#TIME}, otherwise
   * will throw an exception.
   *
   * @param name
   *          the group name
   * @param time
   *          the data time stamp
   * @param value
   *          the data value
   */
  public void write(String name, Long time, String key, String value) {
    Cask360Group group = groups.get(name);
    if (group != null) {
      group.getDataAsTime().put(time, key, value);
    } else {
      Cask360GroupDataTime dataTime = new Cask360GroupDataTime();
      dataTime.put(time, key, value);
      group = new Cask360Group(name, dataTime);
      groups.put(name, group);
    }
  }

  /**
   * Write the specified time data map to the group with the specified name.
   * <p>
   * Overwrites all existing data (all keys/values) for any times in the
   * specified data for this group.
   * <p>
   * Assumes the group is of the type {@link Cask360GroupType#TIME}, otherwise
   * will throw an exception.
   *
   * @param name
   *          the group name
   * @param data
   *          the data
   */
  public void writeTime(String name, Map<Long, Map<String, String>> data) {
    Cask360Group group = groups.get(name);
    if (group != null) {
      group.getDataAsTime().putAll(data);
    } else {
      Cask360GroupDataTime dataTime = new Cask360GroupDataTime();
      dataTime.putAll(data);
      group = new Cask360Group(name, dataTime);
      groups.put(name, group);
      return;
    }
  }

  /**
   * Writes the data from the specified entity into this entity's data. Ignores
   * ID.
   * <p>
   * Throws an exception if any of the group types don't match.
   * <p>
   * Mimics the write merge behavior of {@link Cask360Table}.
   *
   * @param other
   */
  public void write(Cask360Entity other) {
    for (Map.Entry<String, Cask360Group> entry : other.getGroups().entrySet()) {
      Cask360Group group = groups.get(entry.getKey());
      if (group != null) {
        group.getData().put(entry.getValue().getData());
      } else {
        group = new Cask360Group(entry.getKey(), entry.getValue().getType());
        group.getData().put(entry.getValue().getData());
        groups.put(entry.getKey(), group);
      }
    }
  }

  /**
   * Custom comparator that deep-walks the data to ensure they are exactly the
   * same.
   */
  @Override
  public int compareTo(Cask360Entity other) {
    if (other == null) {
      return 1;
    }
    // Confirm same id
    if (!id.equals(other.id)) {
      return id.compareTo(other.id);
    }
    // Confirm same number of groups
    if (groups.size() != other.groups.size()) {
      return groups.size() > other.groups.size() ? 1 : -1;
    }
    // Iterate groups and verify each is completely the same
    for (Map.Entry<String, Cask360Group> entry : groups.entrySet()) {
      Cask360Group thisGroup = entry.getValue();
      Cask360Group otherGroup = other.getGroups().get(entry.getKey());
      // Group must exist
      if (otherGroup == null) {
        return 1;
      }
      int ret = thisGroup.compareTo(otherGroup);
      if (ret != 0) {
        return ret;
      }
    }
    return 0;
  }

  /**
   * Custom equals that deep-walks the data to ensure they are exactly the same.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof Cask360Entity)) {
      return false;
    }
    Cask360Entity other = (Cask360Entity) o;
    return compareTo(other) == 0;
  }

  public boolean isEmpty() {
    return groups == null || groups.isEmpty();
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }

  /**
   * Parses the specified string into a {@link Cask360Entity}.
   * <p>
   * Accepts two formats for the string.
   * <ul>
   * <li><b>CSV-MAP</b> id,group,key,value</li>
   * <li><b>CSV-TIME</b> id,group,time,key,value</li>
   * <li><b>JSON-MAP</b>
   * {'id':'the_id','data':{'group':{'type':'map','data':{'key':'value'}}}}</li>
   * <li><b>JSON-TIME</b>
   * {'id':'the_id','data':{'group':{'type':'time','data':[{'time':1234,'value':
   * 'value'}]}}}</li>
   * </ul>
   *
   * @param body
   * @return
   * @throws ParseException
   */
  public static Cask360Entity fromString(String body) throws JsonSyntaxException, IllegalArgumentException {
    if (body.startsWith("{")) {
      // Attempt to parse as JSON
      return gson.fromJson(body, Cask360Entity.class);
    }
    // Attempt to parse as CSV
    String[] fields = body.split(",");
    if ((fields.length != 4) && (fields.length != 5)) {
      throw new IllegalArgumentException("Invalid CSV, expected 4 or 5 fields, " + "found " + fields.length);
    }
    Cask360Entity entity = new Cask360Entity(fields[0]);
    if (fields.length == 5) {
      // Time
      Long time = Long.valueOf(fields[2]);
      entity.write(fields[1], time, fields[3], fields[4]);
    } else {
      entity.write(fields[1], fields[2], fields[3]);
    }
    return entity;
  }

  public static Gson getGson() {
    return new GsonBuilder().registerTypeAdapter(Cask360Entity.class, new Cask360EntityJsonSerializer()).create();
  }

  /**
   * Serialize and deserialize {@link Cask360Entity} objects to/from JSON.
   */
  public static class Cask360EntityJsonSerializer
      implements JsonSerializer<Cask360Entity>, JsonDeserializer<Cask360Entity> {

    @Override
    public JsonElement serialize(Cask360Entity src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject dataObject = new JsonObject();
      for (Cask360Group group : src.getGroups().values()) {
        dataObject.add(group.getName(), group.getData().toJson());
      }
      JsonObject obj = new JsonObject();
      obj.addProperty("id", src.getID());
      obj.add("data", dataObject);
      return obj;
    }

    @Override
    public Cask360Entity deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject obj = json.getAsJsonObject();
      String id = obj.get("id").getAsString();
      JsonObject dataObject = obj.get("data").getAsJsonObject();
      Map<String, Cask360Group> groups = new TreeMap<String, Cask360Group>();
      for (Map.Entry<String, JsonElement> entry : dataObject.entrySet()) {
        Cask360Group group = new Cask360Group(entry.getKey(),
            Cask360GroupData.fromJson(entry.getValue().getAsJsonObject()));
        groups.put(entry.getKey(), group);
      }
      return new Cask360Entity(id, groups);
    }
  }

  /**
   * Calculates and returns the total number of individual data elements in this
   * instance of a {@link Cask360Entity}.
   * <p>
   * The number of data elements is calculated by adding the number of key-value
   * pairs in any map-type groups and the number of timestamp-key-value pairs
   * in any time-type groups.
   * <p>
   * This number should line up with the number of {@link Cask360Record}s
   * generated when running SQL queries on a {@link Cask360Table}.
   * @return total number of data elements in this entity instance
   */
  public int size() {
    int size = 0;
    for (Map.Entry<String, Cask360Group> entry : groups.entrySet()) {
      size += entry.getValue().size();
    }
    return size;
  }
}

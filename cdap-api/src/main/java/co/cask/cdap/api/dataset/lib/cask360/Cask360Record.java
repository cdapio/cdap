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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.text.ParseException;

/**
 * Represents a single group-key-value of a single entity in a
 * {@link Cask360Table} or for a {@link Cask360Entity}. This is the record used
 * for the flattened SQL table schema used in {@link Cask360Table}.
 */
public class Cask360Record implements Comparable<Cask360Record> {

  private static final Gson gson = new Gson();

  /** Entity ID */
  String id;

  /** Entity Group Data Name */
  String group;

  /** Entity Group Data Type */
  String type;

  /** Timestamp of Entry if Time Type */
  Long ts;

  /** Data Key if Map Type */
  String key;

  /** Data Value */
  String value;

  public Cask360Record() {
    this("", "", "", 0L, "", "");
  }

  /**
   * Constructs a new instance of a {@link Cask360Record}.
   *
   * @param id
   *          the id of the entity
   * @param group
   *          the data group name
   * @param key
   *          the data key
   * @param value
   *          the data value
   */
  public Cask360Record(String id, String group, String key, String value) {
    this(id, group, "map", 0L, key, value);
  }

  /**
   * Constructs a new instance of a {@link Cask360Record}.
   *
   * @param id
   *          the id of the entity
   * @param group
   *          the data group name
   * @param ts
   *          the data time stamp
   * @param key
   *          the data key
   * @param value
   *          the data value
   */
  public Cask360Record(String id, String group, Long ts, String key, String value) {
    this(id, group, "time", ts, key, value);
  }

  /**
   * Constructs a new instance of a {@link Cask360Record}.
   *
   * @param id
   *          the id of the entity
   * @param group
   *          the data group name
   * @param ts
   *          the data time stamp
   * @param key
   *          the data key
   * @param value
   *          the data value
   */
  public Cask360Record(String id, String group, String type, Long ts, String key, String value) {
    this.id = id;
    this.group = group;
    this.type = type;
    this.ts = ts;
    this.key = key;
    this.value = value;
  }

  public String getID() {
    return this.id;
  }

  public String getGroup() {
    return this.group;
  }

  public String getGroupType() {
    return this.type;
  }

  public long getTs() {
    return this.ts;
  }

  public String getKey() {
    return this.key;
  }

  public String getValue() {
    return this.value;
  }

  /**
   * Custom comparator that deep-walks the data to ensure they are exactly the
   * same.
   */
  @Override
  public int compareTo(Cask360Record other) {
    if (other == null) {
      return 1;
    }
    int cmp = this.id.compareTo(other.id);
    if (cmp != 0) {
      return cmp;
    }
    cmp = this.group.compareTo(other.group);
    if (cmp != 0) {
      return cmp;
    }
    cmp = this.ts.compareTo(other.ts);
    if (cmp != 0) {
      return cmp;
    }
    cmp = this.key.compareTo(other.key);
    if (cmp != 0) {
      return cmp;
    }
    return this.value.compareTo(other.value);
  }

  /**
   * Custom equals that deep-walks the data to ensure they are exactly the same.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof Cask360Record)) {
      return false;
    }
    Cask360Record other = (Cask360Record) o;
    return this.compareTo(other) == 0;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }

  /**
   * Parses the specified string into a {@link Cask360Record}.
   * <p>
   * Accepts two formats for the string.
   * <ul>
   * <li><b>CSV</b> <i>id,group,key,value</i> or <i>id,group,ts,key,value</i></li>
   * <li><b>JSON</b>{'id':'the_id','group':'the_group",'key':'the_key','value':'the_value','ts':123}(ts optional)</li>
   * </ul>
   *
   * @param body
   * @return
   * @throws ParseException
   */
  public static Cask360Record fromString(String body) throws JsonSyntaxException, IllegalArgumentException {
    if (body.startsWith("{")) {
      // Attempt to parse as JSON
      return gson.fromJson(body, Cask360Record.class);
    }
    // Attempt to parse as CSV
    String[] fields = body.split(",");
    if ((fields.length != 4) && (fields.length != 5)) {
      throw new IllegalArgumentException("Invalid CSV, expected 4 fields and found " + fields.length);
    }
    return fields.length == 4 ? new Cask360Record(fields[0], fields[1], fields[2], fields[3])
        : new Cask360Record(fields[0], fields[1], Long.parseLong(fields[2]), fields[3], fields[4]);
  }
}

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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataMap;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataTime;

/**
 * Represents a single group of data for a single entity in a
 * {@link Cask360Table}.
 * <p>
 * Contains both the meta data and actual data. An instance of this is uniquely
 * identified by the entity ID (not part of this) and the group name (part of
 * this).
 * <p>
 * This references two underlying classes, {@link Cask360GroupType} for data
 * group type and {@link Cask360GroupData} for data. The type of the group is
 * different depending on the type of data stored in the group (the group type).
 * Currently supported are the types:
 * <ul>
 * <li><b>MAP</b> {@link Cask360GroupType#MAP} (map of string to string)</li>
 * <li><b>TIME</b> {@link Cask360GroupType#TIME} (time descending map to string)</li>
 * </ul>
 */
public class Cask360Group implements Comparable<Cask360Group> {

  /** Group name */
  private String name;

  /** Group type */
  private Cask360GroupType type;

  /** Group data */
  private Cask360GroupData data;

  /**
   * Private constructor.
   * @param name
   *          group name
   * @param type
   *          group type
   * @param data
   *          group data
   */
  private Cask360Group(String name, Cask360GroupType type, Cask360GroupData data) {
    this.name = name;
    this.type = type;
    this.data = data;
  }

  /**
   * Constructs a group of the specified name and type with no initial data.
   *
   * @param name
   *          group name
   * @param type
   *          group type
   */
  public Cask360Group(String name, Cask360GroupType type) {
    this(name, type, type.newDataInstance());
  }

  /**
   * Constructs a group of the specified name and the specified data.
   *
   * @param name
   *          group name
   * @param data
   *          group data
   */
  public Cask360Group(String name, Cask360GroupData data) {
    this(name, data.getType(), data);
  }

  /**
   * Constructs a group of the specified name and the specified map data.
   *
   * @param name
   *          group name
   * @param data
   *          group data
   */
  public Cask360Group(String name, Cask360GroupDataMap data) {
    this(name, Cask360GroupType.MAP, new Cask360GroupData(Cask360GroupType.MAP, data));
  }

  /**
   * Constructs a group of the specified name and the specified time data.
   *
   * @param name
   *          group name
   * @param data
   *          group data
   */
  public Cask360Group(String name, Cask360GroupDataTime data) {
    this(name, Cask360GroupType.TIME, new Cask360GroupData(Cask360GroupType.TIME, data));
  }

  /**
   * Get the name of this group.
   *
   * @return group string name
   */
  public String getName() {
    return this.name;
  }

  /**
   * Get the type of this group.
   *
   * @return group type
   */
  public Cask360GroupType getType() {
    return this.type;
  }

  /**
   * Get a reference to the data for this group.
   *
   * @return reference to group data
   */
  public Cask360GroupData getData() {
    return this.data;
  }

  /**
   * Get a reference to the data for this group as a map type. Verifies the
   * underlying type is map and the instance is time.
   *
   * @return reference to group data as map type
   */
  public Cask360GroupDataMap getDataAsMap() {
    if (this.type != Cask360GroupType.MAP) {
      throw new IllegalArgumentException("Group type expected to be MAP but is " + this.type.toString());
    }
    if (this.data.getType() != Cask360GroupType.MAP) {
      throw new ClassCastException("Group data is not an instance of " + Cask360GroupDataMap.class.getName());
    }
    return this.data.getDataAsMap();
  }

  /**
   * Get a reference to the data for this group as a time type. Verifies the
   * underlying type is time and the instance is time.
   *
   * @return reference to group data as map type
   */
  public Cask360GroupDataTime getDataAsTime() {
    if (this.type != Cask360GroupType.TIME) {
      throw new IllegalArgumentException("Group type expected to be TIME but is " + this.type.toString());
    }
    if (this.data.getType() != Cask360GroupType.TIME) {
      throw new ClassCastException("Group data is not an instance of " + Cask360GroupDataTime.class.getName());
    }
    return this.data.getDataAsTime();
  }

  @Override
  public int compareTo(Cask360Group other) {
    int cmp = this.name.compareTo(other.getName());
    if (cmp != 0) {
      return cmp;
    }
    cmp = this.type.compareTo(other.getType());
    if (cmp != 0) {
      return cmp;
    }
    return this.data.compareTo(other.getData());
  }

  /**
   * Meta data for a group. Contains name, number, and type.
   * <p>
   * Instances of this are used to persist in the global meta table of
   * {@link Cask360Table} and when processing queries.
   */
  public static class Cask360GroupMeta implements Comparable<Cask360GroupMeta> {

    /** Name of the group */
    private byte[] name;

    /** Number of the group (used to store values, generated per table) */
    private Short number;

    /** Type of data stored in the group */
    private Cask360GroupType type;

    /**
     * Default internal constructor.
     *
     * @param name
     *          group name
     * @param number
     *          group number
     * @param type
     *          group type
     */
    public Cask360GroupMeta(byte[] name, short number, Cask360GroupType type) {
      this.name = name;
      this.number = number;
      this.type = type;
    }

    public byte[] getName() {
      return this.name;
    }

    public short getNumber() {
      return this.number;
    }

    public Cask360GroupType getType() {
      return this.type;
    }

    public byte[] getPrefix() {
      return Cask360GroupMeta.getPrefix(getNumber());
    }

    public static byte[] getPrefix(short number) {
      return Bytes.toBytes(number);
    }

    public byte[] toBytes() {
      return Bytes.add(this.type.toBytes(), Bytes.toBytes(getNumber()), this.name);
    }

    public static Cask360GroupMeta fromBytes(byte[] bytes) {
      if (bytes == null) {
        return null;
      }
      Cask360GroupType type = Cask360GroupType.fromBytes(bytes, 0, Bytes.SIZEOF_BYTE);
      short number = Bytes.toShort(bytes, Bytes.SIZEOF_BYTE, Bytes.SIZEOF_SHORT);
      byte[] name = Bytes.tail(bytes, bytes.length - Bytes.SIZEOF_SHORT - Bytes.SIZEOF_BYTE);
      return new Cask360GroupMeta(name, number, type);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Cask360GroupMeta)) {
        return false;
      }
      return this.compareTo((Cask360GroupMeta) o) == 0;
    }

    @Override
    public int compareTo(Cask360GroupMeta other) {
      if (this.type != other.getType()) {
        return this.type.compareTo(other.getType());
      }
      if (!Bytes.equals(this.name, other.getName())) {
        return Bytes.compareTo(this.name, other.getName());
      }
      if (this.number != other.getNumber()) {
        return Short.compare(this.number, other.getNumber());
      }
      return 0;
    }
  }

  /**
   * Group data types. Currently two types are supported:
   * <ul>
   * <li><b>MAP</b> {@link Cask360GroupType#MAP} (ascending map of string to string)</li>
   * <li><b>TIME</b> {@link Cask360GroupType#TIME} (descending map of long to string)</li>
   * </ul>
   */
  public static enum Cask360GroupType {

    /** Ascending ordered map of string to string */
    MAP,

    /** Descending ordered map of long to string */
    TIME;

    public byte[] toBytes() {
      switch (this) {
      case MAP:
        return new byte[] { (byte) '0' };
      case TIME:
        return new byte[] { (byte) '1' };
      default:
        throw new RuntimeException("Invalid Cask360Group Type");
      }
    }

    public Cask360GroupData newDataInstance() {
      switch (this) {
      case MAP:
        return new Cask360GroupData(this);
      case TIME:
        return new Cask360GroupData(this);
      default:
        throw new RuntimeException("Invalid Cask360Group Type");
      }
    }

    public String toJsonName() {
      switch (this) {
      case MAP:
        return "map";
      case TIME:
        return "time";
      default:
        return null;
      }
    }

    public static Cask360GroupType fromJsonName(String name) {
      if (name.equals("map")) {
        return MAP;
      } else if (name.equals("time")) {
        return TIME;
      } else {
        throw new IllegalArgumentException("Invalid group type (" + name + ")");
      }
    }

    public static Cask360GroupType fromBytes(byte[] bytes) {
      return Cask360GroupType.fromBytes(bytes, 0, 1);
    }

    public static Cask360GroupType fromBytes(byte[] bytes, int offset, int length) {
      if ((length != 1) || (offset > (bytes.length - 1))) {
        throw new RuntimeException("Invalid Cask360GroupType");
      }
      switch (bytes[offset]) {
      case (byte) '0':
        return MAP;
      case (byte) '1':
        return TIME;
      default:
        throw new RuntimeException("Invalid Cask360GroupType");
      }
    }
  }
}

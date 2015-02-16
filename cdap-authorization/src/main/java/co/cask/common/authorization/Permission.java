/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.common.authorization;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;

/**
 * The existence of a permission allows a subject to perform a certain operation (e.g. WRITE) on an object.
 */
public class Permission {

  public static final Permission LIFECYCLE = new Permission("lifecycle", new byte[]{2});
  public static final Permission READ = new Permission("read", new byte[]{3});
  public static final Permission WRITE = new Permission("write", new byte[]{4});
  public static final Permission ADMIN = new Permission("admin", new byte[]{6});
  public static final Permission ANY = new Permission("any", new byte[]{8});

  public static final Permission[] ALL = { READ, LIFECYCLE, WRITE, ADMIN, ANY };

  private static final Map<byte[], Permission> ID_INDEX;
  static {
    ImmutableMap.Builder<byte[], Permission> builder = ImmutableMap.builder();
    for (Permission permission : ALL) {
      builder.put(permission.getId(), permission);
    }
    ID_INDEX = builder.build();
  }

  private static final Map<String, Permission> NAME_INDEX;
  static {
    ImmutableMap.Builder<String, Permission> builder = ImmutableMap.builder();
    for (Permission permission : ALL) {
      builder.put(permission.getName(), permission);
    }
    NAME_INDEX = builder.build();
  }

  private final String name;
  private final byte[] id;

  public Permission(String name, byte[] id) {
    this.name = name;
    this.id = id;
  }

  public static Permission fromId(byte[] id) {
    if (ID_INDEX.containsKey(id)) {
      return ID_INDEX.get(id);
    }

    throw new IllegalArgumentException("Unknown id: " + Arrays.toString(id));
  }

  public static Permission fromName(String name) {
    if (NAME_INDEX.containsKey(name)) {
      return NAME_INDEX.get(name);
    }

    throw new IllegalArgumentException("Unknown name: " + name);
  }

  public String getName() {
    return name;
  }

  public byte[] getId() {
    return id;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name).add("id", Arrays.toString(id)).toString();
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Permission other = (Permission) obj;
    return Objects.equal(this.name, other.name) && Arrays.equals(this.id, other.id);
  }
}

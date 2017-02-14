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

package co.cask.cdap.proto.security;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.proto.id.EntityId;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Identifies a user, group or role which can be authorized to perform an {@link Action} on a {@link EntityId}.
 */
@Beta
public class Principal {
  /**
   * Identifies the type of {@link Principal}.
   */
  public enum PrincipalType {
    USER,
    GROUP,
    ROLE
  }

  private final String name;
  private final PrincipalType type;
  private final int hashCode;
  private final String kerberosPrincipal;
  private final String keytabURI;

  public Principal(String name, PrincipalType type) {
    this(name, type, null, null);
  }

  public Principal(String name, PrincipalType type, @Nullable String kerberosPrincipal, @Nullable String keytabURI) {
    this.name = name;
    this.type = type;
    this.kerberosPrincipal = kerberosPrincipal;
    this.keytabURI = keytabURI;
    this.hashCode = Objects.hash(name, type, kerberosPrincipal, keytabURI);
  }

  public String getName() {
    return name;
  }

  public PrincipalType getType() {
    return type;
  }

  @Nullable
  public String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  @Nullable
  public String getKeytabURI() {
    return keytabURI;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    // check here for instance not the classes so that it can match Role class which extends Principal
    if (!(o instanceof Principal)) {
      return false;
    }

    Principal other = (Principal) o;

    return Objects.equals(name, other.name) && Objects.equals(type, other.type) &&
      Objects.equals(kerberosPrincipal, other.kerberosPrincipal) && Objects.equals(keytabURI, other.keytabURI);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return "Principal{" +
      "name='" + name + '\'' +
      ", type=" + type +
      ", kerberosPrincipal=" + kerberosPrincipal +
      ", keytabURI=" + keytabURI +
      '}';
  }
}

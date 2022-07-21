/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.proto.security;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.proto.id.EntityId;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Identifies a user, group or role which can be authorized to perform an {@link Permission} on a {@link EntityId}.
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
  private final String kerberosPrincipal;
  private final Credential credential;
  // This needs to be transient because we don't want this to be populated during deserialization since that
  // value will not be provided by the client.
  private transient int hashCode;

  public Principal(String name, PrincipalType type) {
    this(name, type, null, null);
  }

  public Principal(String name, PrincipalType type, @Nullable Credential credentials) {
    this(name, type, null, credentials);
  }

  public Principal(String name, PrincipalType type, @Nullable String kerberosPrincipal,
                   @Nullable Credential credential) {
    this.name = name;
    this.type = type;
    this.kerberosPrincipal = kerberosPrincipal;
    this.credential = credential;
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
  public String getCredential() {
    if (credential == null) {
      return null;
    }
    return credential.getValue();
  }

  @Nullable
  public Credential getFullCredential() {
    return credential;
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
      Objects.equals(kerberosPrincipal, other.kerberosPrincipal);
  }

  @Override
  public int hashCode() {
    // There is a one in a Integer.MAX_VALUE chance of the actual hash code being 0. The cache won't work in that case
    // but we are ok with these odds.
    if (hashCode != 0) {
      return hashCode;
    }
    hashCode = Objects.hash(name, type, kerberosPrincipal);
    return hashCode;
  }

  @Override
  public String toString() {
    return "Principal{" +
      "name='" + name + '\'' +
      ", type=" + type +
      ", kerberosPrincipal=" + kerberosPrincipal +
      ", credential=" + credential +
      '}';
  }
}

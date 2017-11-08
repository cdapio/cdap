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

package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Uniquely identifies a Secure store key.
 */
public class SecureKeyId extends NamespacedEntityId implements ParentedId<NamespaceId> {
  // KMS only supports lower case keys.
  private static final Pattern secureKeyNamePattern = Pattern.compile("[a-z0-9_-]+");

  private final String name;
  private transient Integer hashCode;

  public SecureKeyId(String namespace, String name) {
    super(namespace, EntityType.SECUREKEY);
    if (name == null) {
      throw new NullPointerException("Secure key cannot be null.");
    }
    if (!isValidSecureKey(name)) {
      throw new IllegalArgumentException(String.format("Improperly formatted secure key name '%s'." +
                                                         " The name can contain lower case alphabets," +
                                                         " numbers, _, and -", name));
    }
    this.name = name;
  }

  @Override
  public Id toId() {
    throw new UnsupportedOperationException(
      String.format("%s does not have old %s class", SecureKeyId.class.getName(), Id.class.getName()));
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, name));
  }

  @SuppressWarnings("unused")
  public static SecureKeyId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new SecureKeyId(next(iterator, "namespace"), nextAndEnd(iterator, "name"));
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    SecureKeyId secureKeyId = (SecureKeyId) o;
    return Objects.equals(namespace, secureKeyId.namespace) &&
      Objects.equals(name, secureKeyId.name);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, name);
    }
    return hashCode;
  }

  public String getName() {
    return name;
  }

  @Override
  public String getEntityName() {
    return getName();
  }

  private static boolean isValidSecureKey(String name) {
    return secureKeyNamePattern.matcher(name).matches();
  }
}

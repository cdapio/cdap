/*
 * Copyright © 2021 Cask Data, Inc.
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

/**
 * Encapsulating class for credentials passing through CDAP.
 */
public class Credential {
  public static final String CREDENTIAL_TYPE_INTERNAL = "CDAP_credential_internal";
  public static final String CREDENTIAL_TYPE_INTERNAL_PLACEHOLD = "CDAP_credential_internal_placeholder";
  public static final String CREDENTIAL_TYPE_INTERNAL_ENCODED = "CDAP_credential_internal_encoded";
  public static final String CREDENTIAL_TYPE_INTERNAL_LOADREMOTE = "CDAP_credential_internal_loadremote";
  public static final String CREDENTIAL_TYPE_EXTERNAL = "CDAP_credential_external";
  public static final String CREDENTIAL_TYPE_EXTERNAL_ENCRYPTED = "CDAP_credential_external_encrypted";

  /**
   * Identifies the type of credential.
   */
  public enum CredentialType {
    /**
     * Internal credentials will be checked by the internal access enforcer instead of the access enforcer extension.
     */
    INTERNAL(CREDENTIAL_TYPE_INTERNAL),
    INTERNAL_PLACEHOLDER(CREDENTIAL_TYPE_INTERNAL_PLACEHOLD),
    INTERNAL_BASE64_ENCODED(CREDENTIAL_TYPE_INTERNAL_ENCODED),
    INTERNAL_LOAD_REMOTE(CREDENTIAL_TYPE_INTERNAL_LOADREMOTE),
    /**
     * External credentials are credentials which should be checked by the access enforcer extension.
     */
    EXTERNAL(CREDENTIAL_TYPE_EXTERNAL),
    /**
     * External encrypted credentials are credentials which should be decrypted prior to being checked by the
     * access enforcer extension.
     */
    EXTERNAL_ENCRYPTED(CREDENTIAL_TYPE_EXTERNAL_ENCRYPTED);

    private final String qualifiedName;

    CredentialType(String qualifiedName) {
      this.qualifiedName = qualifiedName;
    }

    public String getQualifiedName() {
      return qualifiedName;
    }

    /**
     * Returns the {@link CredentialType} from the qualified name.
     *
     * @param qualifiedName the qualified name
     * @return the credential type
     */
    public static CredentialType fromQualifiedName(String qualifiedName) {
      switch (qualifiedName) {
        case CREDENTIAL_TYPE_INTERNAL:
          return CredentialType.INTERNAL;
        case CREDENTIAL_TYPE_INTERNAL_ENCODED:
          return CredentialType.INTERNAL_BASE64_ENCODED;
        case CREDENTIAL_TYPE_INTERNAL_LOADREMOTE:
          return CredentialType.INTERNAL_LOAD_REMOTE;
        case CREDENTIAL_TYPE_INTERNAL_PLACEHOLD:
          return CredentialType.INTERNAL_PLACEHOLDER;
        case CREDENTIAL_TYPE_EXTERNAL:
          return CredentialType.EXTERNAL;
        case CREDENTIAL_TYPE_EXTERNAL_ENCRYPTED:
          return CredentialType.EXTERNAL_ENCRYPTED;
        default:
          throw new IllegalArgumentException("Invalid qualified credential type");
      }
    }
  }

  private final String value;
  private final CredentialType type;

  public Credential(CredentialType type, String value) {
    this.value = value;
    this.type = type;
  }

  public String getValue() {
    return value;
  }

  public CredentialType getType() {
    return type;
  }

  @Override
  public String toString() {
    return "Credential{" +
      "type=" + type +
      ", length=" + value.length() +
      ", val=" + value +
      "}";
  }
}

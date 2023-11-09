/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

  public static final String CREDENTIAL_TYPE_EXTERNAL_BEARER = "Bearer";
  public static final String CREDENTIAL_TYPE_INTERNAL = "CDAP-Internal";
  public static final String CREDENTIAL_TYPE_EXTERNAL = "CDAP-External";
  public static final String CREDENTIAL_TYPE_EXTERNAL_ENCRYPTED = "CDAP-External-Encrypted";

  /**
   * Identifies the type of credential.
   */
  public enum CredentialType {
    /**
     * Internal credentials will be checked by the internal access enforcer instead of the access
     * enforcer extension.
     */
    INTERNAL(CREDENTIAL_TYPE_INTERNAL),
    /**
     * External credentials are credentials which should be checked by the access enforcer
     * extension.
     */
    EXTERNAL(CREDENTIAL_TYPE_EXTERNAL),
    /**
     * External encrypted credentials are credentials which should be decrypted prior to being
     * checked by the access enforcer extension.
     */
    EXTERNAL_ENCRYPTED(CREDENTIAL_TYPE_EXTERNAL_ENCRYPTED),
    /**
     * External credentials which conform to RFC 6750 Bearer Token Scheme. This should typically
     * only be used when passing tokens outside of CDAP using the Authorization header. See
     * https://www.rfc-editor.org/rfc/rfc6750 for details.
     */
    EXTERNAL_BEARER(CREDENTIAL_TYPE_EXTERNAL_BEARER);

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
  private final Long expirationTimeSecs;

  /**
   * Constructs the Credential.
   *
   * @param value credential value
   * @param type credential type
   */
  public Credential(String value, CredentialType type) {
    this.value = value;
    this.type = type;
    this.expirationTimeSecs = null;
  }

  /**
   * Constructs the Credential.
   *
   * @param value credential value
   * @param type credential type
   * @param expirationTimeSecs the time in seconds after which credential will expire
   */
  public Credential(String value, CredentialType type, Long expirationTimeSecs) {
    this.value = value;
    this.type = type;
    this.expirationTimeSecs = expirationTimeSecs;
  }

  public String getValue() {
    return value;
  }

  public CredentialType getType() {
    return type;
  }

  public Long getExpirationTimeSecs() {
    return expirationTimeSecs;
  }

  @Override
  public String toString() {
    return "Credential{"
        + "type=" + type
        + ", expires_in=" + expirationTimeSecs
        + ", length=" + value.length()
        + "}";
  }
}

/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.auth;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Represents a verified identity used for client authentication.  The token consists of two parts:
 * <ul>
 *   <li>the token identity - represented by the {@link AccessTokenIdentifier}, this contains the authenticated
 *   username, group memberships, and valid token lifetime,</li>
 *   <li>the token digest - a signature computed on the token identity components plus a server-side secret key,
 *   this ensures that the token is authentic (was issued by a trusted server) and has not been modified.</li>
 * </ul>
 *
 * An access token is issued following successful authentication of a client by an external mechanism (such as LDAP,
 * Kerberos, etc).  The token is then provided by the client on subsequent requests to CDAP.  In order to validate
 * the token, CDAP components recompute the digest for the token identifier, based on their own knowledge of the
 * secret key.  If the recomputed digest matches that provided by the client, we know that, barring compromise of the
 * secret keys or exposure of the token itself, the token was issued by CDAP for this client.
 */
public class AccessToken implements Signed<AccessTokenIdentifier> {
  static final class Schemas {
    private static final int VERSION = 1;
    private static final Map<Integer, Schema> schemas = Maps.newHashMap();
    static {
      schemas.put(1,
          Schema.recordOf("AccessToken",
              Schema.Field.of("identifier", AccessTokenIdentifier.Schemas.getSchemaVersion(1)),
              Schema.Field.of("keyId", Schema.of(Schema.Type.INT)),
              Schema.Field.of("digest", Schema.of(Schema.Type.BYTES))));
    }


    public static int getVersion() {
      return VERSION;
    }

    public static Schema getSchemaVersion(int version) {
      return schemas.get(version);
    }

    public static Schema getCurrentSchema() {
      return schemas.get(VERSION);
    }
  }

  private final AccessTokenIdentifier identifier;
  private final int keyId;
  private final byte[] digest;

  public AccessToken(AccessTokenIdentifier identifier, int keyId, byte[] digest) {
    this.identifier = identifier;
    this.keyId = keyId;
    this.digest = digest;
  }

  /**
   * Returns the identifier for the secret key used to sign this token.
   */
  public int getKeyId() {
    return keyId;
  }

  /**
   * Returns the identity portion of the token (username, group memberships, etc).
   */
  public AccessTokenIdentifier getIdentifier() {
    return identifier;
  }

  @Override
  public AccessTokenIdentifier getMessage() {
    return identifier;
  }

  @Override
  public byte[] getDigestBytes() {
    return digest;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AccessToken) {
      AccessToken other = (AccessToken) object;
      return Objects.equal(identifier, other.identifier) &&
        keyId == other.keyId &&
        Bytes.equals(digest, other.digest);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getIdentifier(), getKeyId(), Bytes.hashCode(getDigestBytes()));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("identifier", identifier)
      .add("keyId", keyId)
      .add("digest", Bytes.toStringBinary(digest))
      .toString();
  }
}

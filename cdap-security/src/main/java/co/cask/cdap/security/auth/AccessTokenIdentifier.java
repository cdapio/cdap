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

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Represents a verified user identity.
 */
public class AccessTokenIdentifier {
  static final class Schemas {
    private static final int VERSION = 1;
    private static final Map<Integer, Schema> schemas = Maps.newHashMap();
    static {
      schemas.put(1, Schema.recordOf("AccessTokenIdentifier",
                                     Schema.Field.of("username", Schema.of(Schema.Type.STRING)),
                                     Schema.Field.of("groups", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                                     Schema.Field.of("issueTimestamp", Schema.of(Schema.Type.LONG)),
                                     Schema.Field.of("expireTimestamp", Schema.of(Schema.Type.LONG))));
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

  private final String username;
  private final List<String> groups;
  private final long issueTimestamp;
  private final long expireTimestamp;

  public AccessTokenIdentifier(String username, Collection<String> groups, long issueTimestamp,
                               long expireTimestamp) {
    this.username = username;
    this.groups = ImmutableList.copyOf(groups);
    this.issueTimestamp = issueTimestamp;
    this.expireTimestamp = expireTimestamp;
  }

  /**
   * Returns the username for this identity.
   */
  public String getUsername() {
    return username;
  }

  /**
   * Returns the list of verified group memberships for this user identity.
   */
  public List<String> getGroups() {
    return groups;
  }

  /**
   * Returns the timestamp, in milliseconds, when this token was issued.
   */
  public long getIssueTimestamp() {
    return issueTimestamp;
  }

  /**
   * Returns the timestamp, in milliseconds, when this token will expire.
   */
  public long getExpireTimestamp() {
    return expireTimestamp;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof AccessTokenIdentifier)) {
      return false;
    }
    AccessTokenIdentifier otherToken = (AccessTokenIdentifier) other;

    return Objects.equal(username, otherToken.username) &&
      Objects.equal(groups, otherToken.groups) &&
      issueTimestamp == otherToken.issueTimestamp &&
      expireTimestamp == otherToken.expireTimestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getUsername(),
                            getGroups(),
                            getIssueTimestamp(),
                            getExpireTimestamp());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("username", username)
      .add("groups", groups)
      .add("issueTimestamp", issueTimestamp)
      .add("expireTimestamp", expireTimestamp)
      .toString();
  }
}

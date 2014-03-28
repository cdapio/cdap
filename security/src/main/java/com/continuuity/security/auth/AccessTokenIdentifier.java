package com.continuuity.security.auth;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Represents a verified user identity.
 */
public class AccessTokenIdentifier {
  private String username;
  private List<String> groups;
  private long issueTimestamp;
  private long expireTimestamp;

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

  /**
   * Returns a string concatenating all of the token fields, excluding the token secret.
   */
  public byte[] toBytes() {
    AccessTokenCodec codec = new AccessTokenCodec();
    // should never throw IOException here
    try {
      return codec.encodeIdentifier(this);
    } catch (IOException ioe) {
      throw new IllegalStateException("Error serializing token identifier state", ioe);
    }
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
}

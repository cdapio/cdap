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

package io.cdap.cdap.master.spi.twill;

import javax.annotation.Nullable;

/**
 * Specifies security context of a {@link org.apache.twill.api.TwillRunnable}. This includes its
 * identity, along with userId and groupId.
 */
public class SecurityContext {

  private final Long userId;
  private final Long groupId;
  private final String identity;

  private SecurityContext(@Nullable Long userId, @Nullable Long groupId,
      @Nullable String identity) {
    this.identity = identity;
    this.userId = userId;
    this.groupId = groupId;
  }

  @Nullable
  public Long getUserId() {
    return userId;
  }

  @Nullable
  public Long getGroupId() {
    return groupId;
  }

  @Nullable
  public String getIdentity() {
    return identity;
  }

  /**
   * Builds and returns an instance of {@link SecurityContext}.
   */
  public static class Builder {

    private Long userId;
    private Long groupId;
    private String identity;

    public Builder withUserId(Long userId) {
      this.userId = userId;
      return this;
    }

    public Builder withGroupId(Long groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder withIdentity(String identity) {
      this.identity = identity;
      return this;
    }

    public SecurityContext build() {
      return new SecurityContext(this.userId, this.groupId, this.identity);
    }
  }
}

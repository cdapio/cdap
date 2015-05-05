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

package co.cask.cdap.examples.profiles;

/**
 * The profile of a user. It contains the user id, name, email address,
 * last login time, and time of their last activity.
 */
public class Profile {

  private final String id;
  private final String name;
  private final String email;

  private final Long lastLogin;
  private final Long lastActivity;

  /**
   * Used when a user profile is read from storage.
   */
  public Profile(String id, String name, String email, Long lastLogin, Long lastActivity) {
    this.id = id;
    this.name = name;
    this.email = email;
    this.lastLogin = lastLogin;
    this.lastActivity = lastActivity;
  }

  /**
   * Used when a user is created for the first time.
   * At that time, the user has had no login or activity.
   */
  public Profile(String id, String name, String email) {
    this(id, name, email, null, null);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getEmail() {
    return email;
  }

  public Long getLastLogin() {
    return lastLogin;
  }

  public Long getLastActivity() {
    return lastActivity;
  }
}


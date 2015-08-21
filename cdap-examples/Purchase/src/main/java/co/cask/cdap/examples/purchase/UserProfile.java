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

package co.cask.cdap.examples.purchase;

import java.util.List;

/**
 * This class represents the profile information for the user
 */
public final class UserProfile {

  private final String id;
  private final String firstName;
  private final String lastName;
  private final List<String> categories;

  public UserProfile(String id, String firstName, String lastName, List<String> categories) {
    this.id = id;
    this.firstName = firstName;
    this.lastName = lastName;
    this.categories = categories;
  }

  public String getId() {
    return id;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public List<String> getCategories() {
    return categories;
  }
}

/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

/**
 * Defines an application name and path in repository.
 */
public class ApplicationPath {
  String name;
  String pathInRepository;

  /**
   * Creates a new {@link AppDetailToPush.Builder}.
   */
  public static Builder builder() {
    return new Builder();
  }

  private ApplicationPath(String name, String pathInRepository) {
    this.name = name;
    this.pathInRepository = pathInRepository;
  }

  public String getName() {
    return name;
  }

  public String getPathInRepository() {
    return pathInRepository;
  }

  public static final class Builder {
    private String name;
    private String pathInRepository;

    private Builder() {
      // Only for the builder() method to use
    }

    public Builder setName(String name){
      this.name = name;
      return this;
    }

    public Builder setPathInRepository(String pathInRepository) {
      this.pathInRepository = pathInRepository;
      return this;
    }

    public ApplicationPath build() {
      return new ApplicationPath(name, pathInRepository);
    }
  }
}

/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.objects;

/**
 *
 */
public class Namespace {

  private final String name;
  private final String description;
  private final long generation;

  private Namespace(Builder builder) {
    this.name = builder.name;
    this.description = builder.description;
    this.generation = builder.generation;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public long getGeneration() {
    return generation;
  }

  /**
   * TODO
   */
  public static class Builder {

    private String name;
    private String description;
    private long generation;

    public Builder name(String name) {
      this.name = name;

      return this;
    }

    public Builder version(String description) {
      this.description = description;

      return this;
    }

    public Builder scope(long generation) {
      this.generation = generation;

      return this;
    }

    public Namespace build() {
      return new Namespace(this);
    }
  }
}

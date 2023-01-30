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

package io.cdap.cdap.proto.sourcecontrol;

/**
 * The request class to set or test repository configuration.
 */
public class RepositoryConfigRequest {
  private final boolean test;
  private final RepositoryConfig repository;

  public RepositoryConfigRequest(RepositoryConfig repository, boolean validate) {
    this.repository = repository;
    this.test = validate;
  }

  public RepositoryConfigRequest(RepositoryConfig repository) {
    this.repository = repository;
    this.test = false;
  }

  public boolean shouldTest() {
    return test;
  }

  public RepositoryConfig getRepository() {
    return repository;
  }
}

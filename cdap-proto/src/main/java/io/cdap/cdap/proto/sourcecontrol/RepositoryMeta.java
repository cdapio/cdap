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
 * Metadata for repository configuration.
 */
public class RepositoryMeta {

  private final RepositoryConfig config;
  private final long updatedTimeMillis;

  public RepositoryMeta(RepositoryConfig config, long updatedTimeMillis) {
    this.config = new RepositoryConfig.Builder(config).build();
    this.updatedTimeMillis = updatedTimeMillis;
  }

  public RepositoryConfig getConfig() {
    return config;
  }

  public long getUpdatedTimeMillis() {
    return updatedTimeMillis;
  }
}

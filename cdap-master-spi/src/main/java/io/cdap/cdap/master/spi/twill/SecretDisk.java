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

/**
 * Represents information about a file-based secret used by a twill runnable.
 */
public class SecretDisk {

  private final String name;
  private final String path;

  public SecretDisk(String name, String path) {
    this.name = name;
    this.path = path;
  }

  /**
   * Returns the secret name.
   *
   * @return the secret name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the secret filepath directory.
   *
   * @return the secret filepath directory
   */
  public String getPath() {
    return path;
  }
}

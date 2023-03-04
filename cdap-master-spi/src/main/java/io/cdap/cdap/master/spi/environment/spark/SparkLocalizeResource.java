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

package io.cdap.cdap.master.spi.environment.spark;

import java.io.File;
import java.net.URI;

/**
 * Represents a resource that will be localized in master environment.
 */
public class SparkLocalizeResource {

  private final URI uri;
  private final boolean isArchive;

  public SparkLocalizeResource(File file) {
    this(file.toURI(), false);
  }

  public SparkLocalizeResource(File file, boolean isArchive) {
    this(file.toURI(), isArchive);
  }

  public SparkLocalizeResource(URI uri) {
    this(uri, false);
  }

  public SparkLocalizeResource(URI uri, boolean isArchive) {
    this.uri = uri;
    this.isArchive = isArchive;
  }

  public URI getURI() {
    return uri;
  }

  public boolean isArchive() {
    return isArchive;
  }

  @Override
  public String toString() {
    return "SparkLocalizeResource{"
        + "uri=" + uri
        + ", isArchive=" + isArchive
        + '}';
  }
}

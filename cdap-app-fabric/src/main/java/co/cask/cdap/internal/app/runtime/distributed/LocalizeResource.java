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

package co.cask.cdap.internal.app.runtime.distributed;

import com.google.common.base.Objects;

import java.io.File;
import java.net.URI;

/**
 * Represents a resource that will be localized to a Twill container.
 */
public final class LocalizeResource {

  private final URI uri;
  private final boolean archive;

  public LocalizeResource(File file) {
    this(file.toURI(), false);
  }

  public LocalizeResource(URI uri, boolean archive) {
    this.uri = uri;
    this.archive = archive;
  }

  public boolean isArchive() {
    return archive;
  }

  public URI getURI() {
    return uri;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("archive", archive)
      .add("uri", uri)
      .toString();
  }
}

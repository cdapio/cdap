/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common;

import io.cdap.cdap.proto.id.ApplicationReference;


/**
 * Exception thrown when source control metadata for an application is not found.
 */
public class SourceControlMetadataNotFoundException extends NotFoundException {

  private final ApplicationReference appRef;

  /**
   * Constructs a new {@link SourceControlMetadataNotFoundException}.
   *
   * @param appRef The application reference for which the metadata was not found.
   */
  public SourceControlMetadataNotFoundException(ApplicationReference appRef) {
    super("SourceControlMetadata of appId(" + appRef.getApplication()
        + ").getApplication() was not found");
    this.appRef = appRef;
  }
}

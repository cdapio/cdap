/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.runtimejob;

import org.apache.twill.api.LocalFile;

import java.util.function.Supplier;

/**
 * This class has file key and a way to create a LocalFile if needed.
 * If caching files, first cache presense can be checked using file key and then if cache does not have a file,
 * fiel can be created and added to the key.
 */
public class LocalFileDescription
{
  /**
   * File key that can be used as a file name. Usually contains simple file name plus unique value that identifies
   * file version. The value can be file version if known or file has if explicit unique version is not known.
   */
  private final String fileKey;
  private final Supplier<LocalFile> fileSupplier;
  /**
   * Indicates if this file should be cached. Run-specific configuration files should not be cached.
   */
  private final boolean cacheable;

  public LocalFileDescription(String fileKey, Supplier<LocalFile> fileSupplier, boolean cacheable) {
    this.fileKey = fileKey;
    this.fileSupplier = fileSupplier;
    this.cacheable = cacheable;
  }

  public LocalFileDescription(String fileKey, Supplier<LocalFile> fileSupplier) {
    this(fileKey, fileSupplier, true);
  }

  public String getFileKey() {
    return fileKey;
  }

  public Supplier<LocalFile> getFileSupplier() {
    return fileSupplier;
  }

  public boolean isCacheable() {
    return cacheable;
  }
}

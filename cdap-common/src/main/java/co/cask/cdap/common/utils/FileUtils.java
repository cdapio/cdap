/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.utils;

import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

/**
 * Utility class for File-related operations.
 */
public class FileUtils {

  /**
   * FileAttribute representing owner-only read/write (600). Note that this should not be used to create directories.
   */
  public static final FileAttribute<Set<PosixFilePermission>> OWNER_ONLY_RW =
    PosixFilePermissions.asFileAttribute(EnumSet.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ));

  private FileUtils(){ }

}

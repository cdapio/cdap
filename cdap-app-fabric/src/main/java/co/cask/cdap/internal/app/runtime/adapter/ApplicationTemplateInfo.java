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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.proto.ProgramType;
import com.google.common.hash.HashCode;

import java.io.File;
import java.io.IOException;

/**
 * Holds information about an ApplicationTemplate.
 */
public final class ApplicationTemplateInfo {
  private final File file;
  private final String name;
  private final String description;
  private final ProgramType programType;
  private final HashCode fileHash;

  public ApplicationTemplateInfo(File file, String name, String description, ProgramType programType,
                                 HashCode fileHash) throws IOException {
    this.file = file;
    this.name = name;
    this.description = description;
    this.programType = programType;
    this.fileHash = fileHash;
  }

  public File getFile() {
    return file;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public ProgramType getProgramType() {
    return programType;
  }

  public HashCode getFileHash() {
    return fileHash;
  }
}

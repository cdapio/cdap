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

package io.cdap.cdap.support.lib;

import java.util.List;

/**
 * Customer request for a list of files' names EX: "files":["testpipeline/82b21b3e-11c6-11ec-81af-0000009bb312.json",
 * "testpipeline/status.json"]
 */
public class SupportBundleRequestFileList {

  private final List<String> files;

  public SupportBundleRequestFileList(List<String> files) {
    this.files = files;
  }

  public List<String> getFiles() {
    return files;
  }
}

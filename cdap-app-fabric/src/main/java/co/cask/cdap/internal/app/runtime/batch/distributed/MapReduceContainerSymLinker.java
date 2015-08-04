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

package co.cask.cdap.internal.app.runtime.batch.distributed;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * TODO(CDAP-3119) This class is part of TWILL-144 hack, need to remove.
 */
public final class MapReduceContainerSymLinker {

  /**
   * Creates symlink from the source path to the target path. If the target path is a symlink, it will get resolved.
   * This method should be called from TwillLauncher only.
   */
  @SuppressWarnings("unuseds")
  public static void symlink(String source, String target) {
    Path sourcePath = Paths.get(source);
    Path targetPath = Paths.get(target);
    try {
      while (Files.isSymbolicLink(targetPath)) {
        targetPath = Files.readSymbolicLink(targetPath);
      }
      Files.createSymbolicLink(sourcePath, targetPath);
    } catch (Exception e) {
      System.out.println("Failed to create symlink from " + sourcePath + " to " + targetPath);
      e.printStackTrace(System.out);
    }
  }

  private MapReduceContainerSymLinker() {
    //
  }
}

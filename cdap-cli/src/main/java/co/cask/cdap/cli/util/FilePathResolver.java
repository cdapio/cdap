/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.util;

import com.google.common.base.Joiner;
import com.google.inject.Inject;

import java.io.File;
import java.util.LinkedList;
import javax.annotation.Nullable;

/**
 * Utilites for file path input.
 */
public class FilePathResolver {

  public static final String CDAP_HOME = "CDAP_HOME";

  private final File workingDir;
  private final File homeDir;

  @Nullable
  private final File cdapHomeDir;

  @Inject
  public FilePathResolver() {
    this(new File(System.getProperty("user.home")), new File(System.getProperty("user.dir")),
         System.getenv(CDAP_HOME) == null ? null : new File(System.getenv(CDAP_HOME)));
  }

  FilePathResolver(File homeDir, File workingDir, @Nullable File cdapHomeDir) {
    this.homeDir = homeDir;
    this.workingDir = workingDir;
    this.cdapHomeDir = cdapHomeDir;
  }

  /**
   * Resolves a bash-style file path into a {@link File}.
   *
   * Handles ".", "..", and "~".
   *
   * @param path bash-style path
   * @return {@link File} of the resolved path
   */
  public File resolvePathToFile(String path) {
    path = resolveVariables(path);

    if (path.contains("/") || path.contains("\\")) {
      path = path.replace("/", File.separator);
      path = path.replace("\\", File.separator);
    }

    // resolve "~"
    if (path.startsWith("~" + File.separator)) {
      path = new File(homeDir, path.substring(2)).getAbsolutePath();
    }

    // turn relative paths into absolute
    if (!new File(path).isAbsolute()) {
      path = new File(workingDir, path).getAbsolutePath();
    }

    // resolve the "." and ".." in the path
    String[] tokens = path.split(File.separator);
    LinkedList<String> finalTokens = new LinkedList<>();
    for (String token : tokens) {
      if (token.equals("..")) {
        if (!finalTokens.isEmpty()) {
          finalTokens.removeLast();
        }
      } else if (!token.equals(".")) {
        finalTokens.addLast(token);
      }
    }

    return new File(File.separator + Joiner.on(File.separator).join(finalTokens));
  }

  private String resolveVariables(String path) {
    if (cdapHomeDir != null) {
      path = path.replaceAll("\\$" + CDAP_HOME + "(?=[^a-zA-Z0-9_])", cdapHomeDir.getAbsolutePath());
    }

    return path;
  }

}

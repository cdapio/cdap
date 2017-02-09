/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.cli.completer;

import jline.console.completer.FileNameCompleter;
import jline.internal.Configuration;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.List;

import static jline.internal.Preconditions.checkNotNull;

/**
 * FileNameCompleter that escapes spaces in path
 */
public class FileNameCompleterWithEscapedSpace extends FileNameCompleter {

  private static final boolean OS_IS_WINDOWS;

  static {
    String os = Configuration.getOsName();
    OS_IS_WINDOWS = os.contains("windows");
  }

  @Override
  public int complete(String buffer, final int cursor, final List<CharSequence> candidates) {
    // buffer can be null
    checkNotNull(candidates);

    if (buffer == null) {
      buffer = "";
    }

    if (buffer.contains("\\ ")) {
      buffer = buffer.replace("\\ ", " ");
    }

    if (OS_IS_WINDOWS) {
      buffer = buffer.replace('/', '\\');
    }

    String translated = buffer;

    File homeDir = getUserHome();

    // Special character: ~ maps to the user's home directory
    if (translated.startsWith("~" + separator())) {
      translated = homeDir.getPath() + translated.substring(1);
    } else if (translated.startsWith("~")) {
      translated = homeDir.getParentFile().getAbsolutePath();
    } else if (!(new File(translated).isAbsolute())) {
      String cwd = getUserDir().getAbsolutePath();
      translated = cwd + separator() + translated;
    }

    File file = new File(translated);
    final File dir;

    if (translated.endsWith(separator())) {
      dir = file;
    } else {
      dir = file.getParentFile();
    }

    File[] entries = dir == null ? new File[0] : dir.listFiles();

    return matchFiles(buffer, translated, entries, candidates);
  }

  @Override
  protected int matchFiles(final String buffer, final String translated, final File[] files,
                           final List<CharSequence> candidates) {
    if (files == null) {
      return -1;
    }

    int matches = 0;

    // first pass: just count the matches
    for (File file : files) {
      if (file.getAbsolutePath().startsWith(translated)) {
        matches++;
      }
    }
    for (File file : files) {
      if (file.getAbsolutePath().startsWith(translated)) {
        CharSequence name = file.getName() + (matches == 1 && file.isDirectory() ? separator() : " ");
        candidates.add(render(file, name).toString().replace(" ", "\\ "));
      }
    }

    final int index = buffer.lastIndexOf(separator()) + StringUtils.countMatches(buffer, " ");

    return index + separator().length();
  }
}

/*
 * Copyright 2012-2014 Cask, Inc.
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

package co.cask.cdap.shell.completer;

import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Completer that forwards completion to another completer only if a prefix is met.
 */
public class PrefixCompleter implements Completer {

  private final String prefix;
  private final Completer completer;

  public PrefixCompleter(String prefix, Completer completer) {
    this.prefix = prefix;
    this.completer = completer;
  }

  @Override
  public int complete(String buffer, int cursor, List<CharSequence> candidates) {
    String prefix = "";
    if (this.prefix != null && !this.prefix.isEmpty()) {
      // TODO: fix bug where prefix has a space in it (e.g. "describe dataset type <type-name>"
      prefix = this.prefix
        .replaceAll("\\{\\}", "\\\\S+?")
        .replaceAll(" ", "\\\\s+?") + " ";
    }

    String regex = "^(" + prefix + ")\\s?";
    Pattern pattern = Pattern.compile(regex);

    if (buffer != null) {
      Matcher matcher = pattern.matcher(buffer);
      if (matcher.find()) {
        String realPrefix = matcher.group();
        String childBuffer = buffer.substring(realPrefix.length()).trim();
        List<CharSequence> childCandidates = Lists.newArrayList();
        int result = completer.complete(childBuffer, cursor - realPrefix.length(), childCandidates);

        for (CharSequence childCandidate : childCandidates) {
          candidates.add(childCandidate.toString().trim());
        }

        if (candidates.size() == 1) {
          candidates.set(0, candidates.get(0) + " ");
        }

        return candidates.isEmpty() ? -1 : realPrefix.length() + result;
      }
    }

    return candidates.isEmpty() ? -1 : 0;
  }
}

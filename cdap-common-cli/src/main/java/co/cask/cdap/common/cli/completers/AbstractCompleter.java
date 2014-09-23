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

package co.cask.cdap.common.cli.completers;

import jline.console.completer.Completer;

import java.util.Collection;
import java.util.List;

import static jline.internal.Preconditions.checkNotNull;

/**
 * Completer that provides an alternative API to implement {@link Completer}.
 */
public abstract class AbstractCompleter implements Completer {

  protected abstract Collection<String> getAllCandidates();
  protected abstract Collection<String> getCandidates(String buffer);

  @Override
  public int complete(String buffer, int cursor, List<CharSequence> candidatesOut) {
    checkNotNull(candidatesOut);

    if (buffer == null) {
      candidatesOut.addAll(getAllCandidates());
    } else {
      Collection<String> candidates = getCandidates(buffer);
      for (String candidate : candidates) {
        candidatesOut.add(candidate);
      }
    }

    if (candidatesOut.size() == 1) {
      candidatesOut.set(0, candidatesOut.get(0) + " ");
    }

    return candidatesOut.isEmpty() ? -1 : 0;
  }
}

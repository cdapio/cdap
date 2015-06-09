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

package co.cask.cdap.cli.completer;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import jline.console.completer.Completer;

import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static jline.internal.Preconditions.checkNotNull;

/**
 * Completer for a set of strings.
 */
public class StringsCompleter implements Completer {

  private final Supplier<Collection<String>> strings;

  public StringsCompleter(Supplier<Collection<String>> strings) {
    checkNotNull(strings);
    this.strings = Suppliers.memoizeWithExpiration(strings, 5000, TimeUnit.MILLISECONDS);
  }

  public StringsCompleter(Collection<String> strings) {
    checkNotNull(strings);
    this.strings = Suppliers.ofInstance(strings);
  }

  public TreeSet<String> getStrings() {
    return new TreeSet<>(strings.get());
  }

  @Override
  public int complete(@Nullable final String buffer, final int cursor, final List<CharSequence> candidates) {
    checkNotNull(candidates);

    TreeSet<String> strings = getStrings();
    if (buffer == null) {
      candidates.addAll(strings);
    } else {
      for (String match : strings.tailSet(buffer)) {
        if (!match.startsWith(buffer)) {
          break;
        }

        candidates.add(match);
      }
    }

    if (candidates.size() == 1) {
      candidates.set(0, candidates.get(0) + " ");
    }

    return candidates.isEmpty() ? -1 : 0;
  }
}

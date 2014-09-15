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

package co.cask.cdap.shell.completer;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import jline.console.completer.Completer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static jline.internal.Preconditions.checkNotNull;

/**
 * Completer for a set of strings.
 */
public abstract class StringsCompleter extends AbstractCompleter {

  protected abstract Supplier<Collection<String>> getStringsSupplier();

  @Override
  protected Collection<String> getAllCandidates() {
    return getStrings();
  }

  @Override
  protected Collection<String> getCandidates(String buffer) {
    ImmutableList.Builder<String> candidates = ImmutableList.builder();

    for (String candidate : getStrings()) {
      if (candidate.startsWith(buffer)) {
        candidates.add(candidate);
      }
    }

    return candidates.build();
  }

  public Collection<String> getStrings() {
    return getStringsSupplier().get();
  }
}

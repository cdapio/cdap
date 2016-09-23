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

package co.cask.cdap.cli.completer.element;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.completer.StringsCompleter;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Provider;

import java.util.Collection;
import java.util.List;
import javax.inject.Inject;

/**
 * Completer for application IDs.
 */
public class ArtifactNameCompleter extends StringsCompleter {

  @Inject
  ArtifactNameCompleter(final Provider<ArtifactClient> artifactClient, final CLIConfig cliConfig) {
    super(new Supplier<Collection<String>>() {
      @Override
      public Collection<String> get() {
        try {
          List<ArtifactSummary> artifactSummaries = artifactClient.get().list(cliConfig.getCurrentNamespace().toId());
          List<String> names = Lists.newArrayList();
          for (ArtifactSummary summary : artifactSummaries) {
            names.add(summary.getName());
          }
          return names;
        } catch (Exception e) {
          return Lists.newArrayList();
        }
      }
    });
  }
}

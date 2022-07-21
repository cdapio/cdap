/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.net.URI;
import java.util.Optional;

/**
 * The {@link TwillApplication} for launch preview runner.
 */
public class PreviewRunnerTwillApplication implements TwillApplication {

  static final String NAME = "preview.runner";

  private final URI cConfFileURI;
  private final URI hConfFileURI;
  private final ResourceSpecification resourceSpec;
  private final Optional<ResourceSpecification> artifactLocalizerResourceSpec;

  public PreviewRunnerTwillApplication(URI cConfFileURI, URI hConfFileURI, ResourceSpecification resourceSpec,
                                       Optional<ResourceSpecification> artifactLocalizerResourceSpec) {
    this.cConfFileURI = cConfFileURI;
    this.hConfFileURI = hConfFileURI;
    this.resourceSpec = resourceSpec;
    this.artifactLocalizerResourceSpec = artifactLocalizerResourceSpec;
  }

  @Override
  public TwillSpecification configure() {
    TwillSpecification.Builder.MoreRunnable runnables = TwillSpecification.Builder.with()
      .setName(NAME)
      .withRunnable();

    TwillSpecification.Builder.RunnableSetter runnableSetter =
    runnables.add(new PreviewRunnerTwillRunnable("cConf.xml", "hConf.xml"), resourceSpec)
      .withLocalFiles()
        .add("cConf.xml", cConfFileURI)
        .add("hConf.xml", hConfFileURI)
      .apply();

    artifactLocalizerResourceSpec.ifPresent(spec ->
      runnables.add(new ArtifactLocalizerTwillRunnable("cConf.xml", "hConf.xml"), spec)
        .withLocalFiles()
        .add("cConf.xml", cConfFileURI)
        .add("hConf.xml", hConfFileURI)
        .apply());

    return runnableSetter.anyOrder().build();
  }
}

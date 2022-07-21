/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.net.URI;

/**
 * The {@link TwillApplication} for launching task workers along with artifact localizers as sidecar containers.
 */
public class TaskWorkerTwillApplication implements TwillApplication {

  static final String NAME = "task.worker";

  private final URI cConfFileURI;
  private final URI hConfFileURI;
  private final ResourceSpecification taskworkerResourceSpec;
  private final ResourceSpecification artifactLocalizerResourceSpec;

  public TaskWorkerTwillApplication(URI cConfFileURI, URI hConfFileURI,
                                    ResourceSpecification taskworkerResourceSpec,
                                    ResourceSpecification artifactLocalizerResourceSpec) {
    this.cConfFileURI = cConfFileURI;
    this.hConfFileURI = hConfFileURI;
    this.taskworkerResourceSpec = taskworkerResourceSpec;
    this.artifactLocalizerResourceSpec = artifactLocalizerResourceSpec;
  }

  @Override
  public TwillSpecification configure() {
    return TwillSpecification.Builder.with()
      .setName(NAME)
      .withRunnable()
        .add(new TaskWorkerTwillRunnable("cConf.xml", "hConf.xml"), taskworkerResourceSpec)
        .withLocalFiles()
        .add("cConf.xml", cConfFileURI)
        .add("hConf.xml", hConfFileURI)
      .apply()
        .add(new ArtifactLocalizerTwillRunnable("cConf.xml", "hConf.xml"),
             artifactLocalizerResourceSpec)
        .withLocalFiles()
        .add("cConf.xml", cConfFileURI)
        .add("hConf.xml", hConfFileURI)
      .apply()
      .anyOrder()
      .build();
  }
}

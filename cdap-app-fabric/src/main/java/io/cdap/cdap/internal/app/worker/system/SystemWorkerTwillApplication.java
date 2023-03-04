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

package io.cdap.cdap.internal.app.worker.system;

import java.net.URI;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

public class SystemWorkerTwillApplication implements TwillApplication {

  public static final String NAME = "system.worker";

  private final URI cConfFileURI;
  private final URI hConfFileURI;
  private final URI sConfFileURI;
  private final ResourceSpecification systemWorkerResourceSpec;

  public SystemWorkerTwillApplication(URI cConfFileURI, URI hConfFileURI, URI sConfFileURI,
      ResourceSpecification systemWorkerResourceSpec) {
    this.cConfFileURI = cConfFileURI;
    this.hConfFileURI = hConfFileURI;
    this.sConfFileURI = sConfFileURI;
    this.systemWorkerResourceSpec = systemWorkerResourceSpec;
  }

  @Override
  public TwillSpecification configure() {
    return TwillSpecification.Builder.with()
        .setName(NAME)
        .withRunnable()
        .add(new SystemWorkerTwillRunnable("cConf.xml", "hConf.xml", "sConf.xml"),
            systemWorkerResourceSpec)
        .withLocalFiles()
        .add("cConf.xml", cConfFileURI)
        .add("hConf.xml", hConfFileURI)
        .add("sConf.xml", sConfFileURI)
        .apply()
        .anyOrder()
        .build();
  }
}

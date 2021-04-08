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

package io.cdap.cdap.internal.app.dispatcher;

import io.cdap.cdap.common.conf.Constants;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.net.URI;

/**
 * The {@link TwillApplication} for launch task workers
 */
public class TaskWorkerTwillApplication implements TwillApplication {

  static final String NAME = "task.worker";

  private final URI cConfFileURI;
  private final URI hConfFileURI;
  private final ResourceSpecification resourceSpec;

  public TaskWorkerTwillApplication(URI cConfFileURI, URI hConfFileURI, ResourceSpecification resourceSpec) {
    this.cConfFileURI = cConfFileURI;
    this.hConfFileURI = hConfFileURI;
    this.resourceSpec = resourceSpec;
  }

  @Override
  public TwillSpecification configure() {
    return TwillSpecification.Builder.with()
      .setName(NAME)
      .withRunnable()
        .add(new TaskWorkerTwillRunnable("cConf.xml", "hConf.xml"), resourceSpec)
      .withLocalFiles()
        .add("cConf.xml", cConfFileURI)
        .add("hConf.xml", hConfFileURI)
      .apply()
      .anyOrder()
      .build();
  }
}

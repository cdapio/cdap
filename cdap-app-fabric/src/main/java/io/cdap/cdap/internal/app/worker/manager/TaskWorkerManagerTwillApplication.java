/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.manager;

import java.net.URI;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

/**
 * The {@link TwillApplication} for launching the task worker manager proxy. Kept separate from
 * {@code TaskWorkerTwillApplication} because the proxy is a singleton and must not roll with the
 * pool.
 */
public class TaskWorkerManagerTwillApplication implements TwillApplication {

  static final String NAME = "task.worker.manager";

  private final URI cConfFileUri;
  private final URI hConfFileUri;
  private final ResourceSpecification resourceSpec;

  public TaskWorkerManagerTwillApplication(URI cConfFileUri, URI hConfFileUri,
      ResourceSpecification resourceSpec) {
    this.cConfFileUri = cConfFileUri;
    this.hConfFileUri = hConfFileUri;
    this.resourceSpec = resourceSpec;
  }

  URI getCConfFileUri() {
    return cConfFileUri;
  }

  URI getHConfFileUri() {
    return hConfFileUri;
  }

  @Override
  public TwillSpecification configure() {
    return TwillSpecification.Builder.with()
        .setName(NAME)
        .withRunnable()
        .add(new TaskWorkerManagerTwillRunnable("cConf.xml", "hConf.xml"), resourceSpec)
        .withLocalFiles()
        .add("cConf.xml", cConfFileUri)
        .add("hConf.xml", hConfFileUri)
        .apply()
        .anyOrder()
        .build();
  }
}

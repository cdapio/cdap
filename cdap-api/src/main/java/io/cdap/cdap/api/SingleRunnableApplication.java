/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api;

import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillSpecification;

/**
 * A simple {@link org.apache.twill.api.TwillApplication} that contains
 * only one {@link org.apache.twill.api.TwillRunnable}.
 *
 */
public class SingleRunnableApplication implements TwillApplication {

  private final TwillRunnable runnable;
  private final ResourceSpecification resourceSpec;

  public SingleRunnableApplication(TwillRunnable runnable, ResourceSpecification resourceSpec) {
    this.runnable = runnable;
    this.resourceSpec = resourceSpec;
  }

  @Override
  public TwillSpecification configure() {
    TwillRunnableSpecification runnableSpec = runnable.configure();
    return TwillSpecification.Builder.with()
      .setName(runnableSpec.getName())
      .withRunnable().add(runnableSpec.getName(), runnable, resourceSpec)
      .noLocalFiles()
      .anyOrder()
      .build();
  }
}

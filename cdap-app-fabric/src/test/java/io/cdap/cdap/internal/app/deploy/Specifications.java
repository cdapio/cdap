/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy;

import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.DefaultAppConfigurer;
import io.cdap.cdap.app.DefaultApplicationContext;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.DefaultId;

/**
 * Util for building app spec for tests.
 */
public final class Specifications {
  private Specifications() {}

  public static ApplicationSpecification from(Application app) {
    DefaultAppConfigurer appConfigurer = new DefaultAppConfigurer(Id.Namespace.fromEntityId(DefaultId.NAMESPACE),
                                                                  Id.Artifact.fromEntityId(DefaultId.ARTIFACT),
                                                                  app);
    app.configure(appConfigurer, new DefaultApplicationContext());
    return appConfigurer.createSpecification(null);
  }
}

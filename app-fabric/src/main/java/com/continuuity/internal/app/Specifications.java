/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app;

import com.continuuity.api.app.Application;
import com.continuuity.api.app.ApplicationContext;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.DefaultAppConfigurer;

/**
 * Util for building app spec for tests.
 */
public final class Specifications {
  private Specifications() {}

  public static ApplicationSpecification from(Application app) {
    DefaultAppConfigurer appConfigurer = new DefaultAppConfigurer(app);
    app.configure(appConfigurer, new ApplicationContext());
    return appConfigurer.createApplicationSpec();
  }
}

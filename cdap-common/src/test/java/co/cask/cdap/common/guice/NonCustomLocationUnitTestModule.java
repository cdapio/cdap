/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
package co.cask.cdap.common.guice;

import co.cask.cdap.common.namespace.DefaultNamespacePathLocator;
import co.cask.cdap.common.namespace.NamespacePathLocator;
import co.cask.cdap.common.namespace.NoLookupNamespacePathLocator;
import co.cask.cdap.proto.NamespaceMeta;

/**
 * Location Factory guice binding for unit tests. It extends from the
 * {@link LocalLocationModule}, and also defines the {@link NamespacePathLocator} binding to
 * {@link NoLookupNamespacePathLocator}, which does not perform {@link NamespaceMeta} lookup like
 * {@link DefaultNamespacePathLocator} and hence in unit tests the namespace does not need to be created to get
 * namespaces locations.
 */
public class NonCustomLocationUnitTestModule extends LocalLocationModule {

  @Override
  protected void configure() {
    super.configure();
    bind(NamespacePathLocator.class).to(NoLookupNamespacePathLocator.class);
  }
}

/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;

/**
 * Class whose super class is not Application Template. To test Adapter creation logic.
 */
public class ExtendedBatchTemplate extends DummyBatchTemplate {

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(ExtendedBatchTemplate.class.getSimpleName());
    // make the description different each time to distinguish between deployed versions in unit tests
    configurer.setDescription("Hello World");
    configurer.addWorkflow(new AdapterWorkflow());
    configurer.addMapReduce(new DummyMapReduceJob());
  }
}

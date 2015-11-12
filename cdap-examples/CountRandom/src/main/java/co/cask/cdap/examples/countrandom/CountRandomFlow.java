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
package co.cask.cdap.examples.countrandom;


import co.cask.cdap.api.flow.AbstractFlow;

/**
 * Count random Flow declaration {@code CountRandomFlow}.
 */
public class CountRandomFlow extends AbstractFlow {

  @Override
  protected void configure() {
    setName("CountRandom");
    setDescription("CountRandom Flow");
    addFlowlet("source", new RandomSource());
    addFlowlet("splitter", new NumberSplitter());
    addFlowlet("counter", new NumberCounter());
    connect("source", "splitter");
    connect("splitter", "counter");
  }
}

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

package co.cask.cdap.common.collect;

import java.util.Collection;

/**
 * This can be used to collect with different strategies while iterating
 * over a stream of elements. For every element in the stream, add the
 * element to the collector. The collector then indicates whether more
 * elements are needed (for instance, to collect the first N elements only,
 * use a collector that returns false after the Nth element has been added.
 *
 * @param <Element> Type of element.
 */
public interface Collector<Element> {
  /**
   * collect one element.
   * @param element the element to collect
   * @return whether more elements need to be collected
   */
  boolean addElement(Element element);

  /**
   * Finish collection of elements and add all collected elements into the given {@link Collection}.
   *
   * @param collection {@link Collection} for storing collected elements.
   * @param <T> Type of collection
   * @return The same {@link Collection} instance given in the parameter.
   */
  <T extends Collection<? super Element>> T finish(T collection);
}

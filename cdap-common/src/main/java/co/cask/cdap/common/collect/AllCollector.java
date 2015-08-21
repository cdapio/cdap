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

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * This collector will collect until it runs out of memory, it
 * never returns false.
 *
 * @param <Element> Type of element
 */
public class AllCollector<Element> implements Collector<Element> {

  private final List<Element> elements = Lists.newArrayList();

  @Override
  public boolean addElement(Element element) {
    elements.add(element);
    return true;
  }

  @Override
  public <T extends Collection<? super Element>> T finish(T collection) {
    collection.addAll(elements);
    elements.clear();
    return collection;
  }
}

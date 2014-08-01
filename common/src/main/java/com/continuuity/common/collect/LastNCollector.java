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

package com.continuuity.common.collect;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Deque;

/**
 * This collector will keep only the most recent N elements. It will
 * never return false, but keeps a bound on the memory it uses.
 *
 * @param <Element> Type of element.
 */
public class LastNCollector<Element> implements Collector<Element> {

  private final Deque<Element> elements;
  private final int maxCount;

  public LastNCollector(int n) {
    Preconditions.checkArgument(n > 0, "n must be greater than 0");
    this.maxCount = n;
    this.elements = Lists.newLinkedList();
  }

  @Override
  public boolean addElement(Element element) {
    if (elements.size() < maxCount) {
      elements.addLast(element);
    } else {
      elements.removeFirst();
      elements.addLast(element);
    }
    return true;
  }

  @Override
  public <T extends Collection<? super Element>> T finish(T collection) {
    collection.addAll(elements);
    elements.clear();
    return collection;
  }
}


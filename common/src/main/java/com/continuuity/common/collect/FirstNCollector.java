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
import com.google.common.collect.ObjectArrays;

import java.util.Arrays;

/**
 * This collector is used for collecting the first N elements, it returns
 * false after N elements have been collected.
 *
 * @param <Element> Type of element.
 */
public class FirstNCollector<Element> implements Collector<Element> {
  private final Element[] elements;
  private int count;

  public FirstNCollector(int n, Class<Element> clazz) {
    Preconditions.checkArgument(n > 0, "n must be greater than 0");
    elements = ObjectArrays.newArray(clazz, n);
  }

  @Override
  public boolean addElement(Element element) {
    if (count >= elements.length) {
      return false;
    }
    elements[count++] = element;
    return (count < elements.length);
  }

  @Override
  public Element[] finish() {
    if (count >= elements.length) {
      return elements;
    }
    return Arrays.copyOf(elements, count);
  }
}


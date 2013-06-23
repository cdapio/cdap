package com.continuuity.common.collect;

import com.google.common.base.Preconditions;
import com.google.common.collect.ObjectArrays;

import java.util.Arrays;

/**
 * This collector is used for collecting the first N elements, it returns
 * false after N elements have been collected
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
    if (count >= elements.length) return false;
    elements[count++] = element;
    return (count < elements.length);
  }

  @Override
  public Element[] finish() {
    if (count >= elements.length) return elements;
    else return Arrays.copyOf(elements, count);
  }
}


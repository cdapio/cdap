package com.continuuity.common.collect;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * This collector is used for collecting the first N elements, it returns
 * false after N elements have been collected
 */
public class FirstNCollector<Element> implements Collector<Element> {
  private Element[] elements;
  private int count = 0;

  @SuppressWarnings("unchecked")
  public FirstNCollector(int n, Class<Element> clazz) {
    if (n < 1) throw new IllegalArgumentException("n must be greater han 0");
    elements = (Element[]) Array.newInstance(clazz, n);
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


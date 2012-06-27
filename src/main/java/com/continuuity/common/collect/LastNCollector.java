package com.continuuity.common.collect;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * This collector will keep only the most recent N elements. It will
 * never return false, but keeps a bound on the memory it uses.
 */
public class LastNCollector<Element> implements Collector<Element> {
  private Element[] elements;
  private int count = 0;

  @SuppressWarnings("unchecked")
  public LastNCollector(int n, Class<Element> clazz) {
    if (n < 1) throw new IllegalArgumentException("n must be greater han 0");
    elements = (Element[]) Array.newInstance(clazz, n);
  }

  @Override
  public boolean addElement(Element element) {
    elements[count % elements.length] = element;
    count++;
    return true;
  }

  @Override
  public Element[] finish() {
    if (count < elements.length) {
      return Arrays.copyOf(elements, count);
    } else {
      // it would be nice to use new Element[] but type erasure prevents that
      Element[] array = Arrays.copyOf(elements, elements.length);
      for (int i = 0; i < elements.length; i++)
        array[i] = elements[(count + i) % elements.length];
      return array;
    }
  }
}


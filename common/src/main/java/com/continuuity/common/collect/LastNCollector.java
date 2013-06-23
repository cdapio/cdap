package com.continuuity.common.collect;

import com.google.common.base.Preconditions;
import com.google.common.collect.ObjectArrays;

import java.util.Arrays;

/**
 * This collector will keep only the most recent N elements. It will
 * never return false, but keeps a bound on the memory it uses.
 */
public class LastNCollector<Element> implements Collector<Element> {
  private final Class<Element> clazz;
  private final Element[] elements;
  private int count = 0;

  public LastNCollector(int n, Class<Element> clazz) {
    Preconditions.checkArgument(n > 0, "n must be greater than 0");
    this.clazz = clazz;
    elements = ObjectArrays.newArray(clazz, n);
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
      int mod = count % elements.length;
      Element[] array = ObjectArrays.newArray(clazz, elements.length);
      System.arraycopy(elements, mod, array, 0, elements.length - mod);
      System.arraycopy(elements, 0, array, elements.length - mod, mod);
      return array;
    }
  }
}


package com.continuuity.common.collect;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;

import java.util.List;

/**
 * This collector will collect until it runs out of memory, it
 * never returns false
 */
public class AllCollector<Element> implements Collector<Element> {

  private final List<Element> elements = Lists.newArrayList();

  private final Class<Element> clazz;

  public AllCollector(Class<Element> clazz) {
    this.clazz = clazz;
  }

  @Override
  public boolean addElement(Element element) {
    elements.add(element);
    return true;
  }

  @Override
  public Element[] finish() {
    Element[] array = ObjectArrays.newArray(clazz, elements.size());
    return elements.toArray(array);
  }
}

package com.continuuity.common.collect;

import com.google.common.collect.Lists;

import java.lang.reflect.Array;
import java.util.ArrayList;

/**
 * This collector will collect until it runs out of memory, it
 * never returns false
 */
public class AllCollector<Element> implements Collector<Element> {

  private ArrayList<Element> elements = Lists.newArrayList();

  Class<Element> clazz;

  public AllCollector(Class<Element> clazz) {
    this.clazz = clazz;
  }

  @Override
  public boolean addElement(Element element) {
    elements.add(element);
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Element[] finish() {
    Element[] array = (Element[]) Array.newInstance(this.clazz, elements.size());
    return elements.toArray(array);
  }
}

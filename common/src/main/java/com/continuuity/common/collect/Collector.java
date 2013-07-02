package com.continuuity.common.collect;

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
  public boolean addElement(Element element);

  /**
   * Finish collection of elements and return all elements that were added.
   * @return all the collected elements
   */
  public Element[] finish();
}

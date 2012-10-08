package com.continuuity.metrics2.common;

import java.util.Collection;
import java.util.Iterator;

/**
 * Implementation of python zip functionality for parallelly iterating
 * through two lists of collection.
 * <blockquote>
 *   <pre>
 *    boolean status = Zip.zip(c1, c2, new ZipIterator<Object, Object>() {
 *      public boolean each(Object o1, Object o2) {
 *        ....
 *        return true;
 *      }
 *    }
 *   </pre>
 * </blockquote>
 */
public class Zip {
  public static <T,U> boolean zip(Collection<T> ct, Collection<U> cu,
                                  ZipIterator<T,U> each) {
    Iterator<T> it = ct.iterator();
    Iterator<U> iu = cu.iterator();
    while (it.hasNext() && iu.hasNext()) {
      if (!each.each(it.next(), iu.next())) {
        return false;
      }
    }
    return !it.hasNext() && !iu.hasNext();
  }
}

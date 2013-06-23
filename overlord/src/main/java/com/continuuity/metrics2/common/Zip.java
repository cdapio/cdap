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

  /**
   * Smart zip that iterates through the two list of timeseries using
   * {@link ZipIterator.Advance}. For each iteration it passed the objects
   * to {@link ZipIterator#each(Object, Object,
   *  com.continuuity.metrics2.common.ZipIterator.Advance)}
   *
   * @param a Collection of objects for list A
   * @param b Collection of objects for list B
   * @param each functor to advance and process objects.
   */
  public static <T, U> void zip(Collection<T> a, Collection<U> b,
                                     ZipIterator<T, U> each) {
    // Initialize iterator to begin.
    Iterator<T> itA = a.iterator();
    Iterator<U> itB = b.iterator();

    T aData = null;
    U bData = null;

    // Set advance to move both.
    ZipIterator.Advance advance = ZipIterator.Advance.BOTH;
    while(itA.hasNext() && itB.hasNext()) {

      // Depending on what gets returned from the advance,
      // move the iterators accordingly.
      if(advance == ZipIterator.Advance.BOTH) {
        aData = itA.next();
        bData = itB.next();
      } else if(advance == ZipIterator.Advance.ITER_A) {
        aData = itA.next();
      } else if(advance == ZipIterator.Advance.ITER_B) {
        bData = itB.next();
      }

      // Pass along each object pair for processing.
      each.each(aData, bData, advance);

      // Figure out how to advance next.
      advance = each.advance(aData, bData);
    }

    // After we are here, one of the list has completed. check which
    // one it is and based on that use the prev value.
    if(itB.hasNext()) {
      while(itB.hasNext()) {
        bData = itB.next();
        each.each(aData, bData, ZipIterator.Advance.ITER_B);
      }
    } else if(itA.hasNext()) {
      while(itA.hasNext()) {
        aData = itA.next();
        each.each(aData, bData, ZipIterator.Advance.ITER_A);
      }
    }
  }
}

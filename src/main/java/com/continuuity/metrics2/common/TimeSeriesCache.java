package com.continuuity.metrics2.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Implements a basic time series cache.
 */
public class TimeSeriesCache<T extends Datnum> {
  /**
   * Id of the series being stored.
   */
  private String seriesId;

  /**
   * Is the series object valid or no.
   */
  private boolean isValid;

  /**
   * Cache of data points.
   */
  private TreeMap<Long, T> cache;

  /**
   * min time available in this cache.
   */
  private long start;

  /**
   * max time available in this cache.
   */
  private long end;

  /**
   * Constructs and initializes the time series cache.
   *
   * @param seriesId of the series.
   */
  public TimeSeriesCache(String seriesId) {
    this.seriesId = seriesId;
    isValid = false;
    cache = new TreeMap<Long, T>();
    resetRangeMarkers();
  }

  /**
   * Clears the cache and range markers.
   */
  public void clear() {
    cache.clear();
    resetRangeMarkers();
  }

  public boolean isValid() {
    return isValid;
  }

  public String getSeriesId() {
    return seriesId;
  }

  public long getStart() {
    return start;
  }

  public void updateStart(long start) {
    if (start < this.start)
      this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void updateEnd(long end) {
    if (end > this.end)
      this.end = end;
  }

  public T addT(T d) {
    if (d.getTimestamp() < start)
      start = d.getTimestamp();
    if (d.getTimestamp() > end)
      end = d.getTimestamp();
    isValid = true;
    return cache.put(d.getTimestamp(), d);
  }

  public T updateT(T d) {
    if (cache.get(d.getTimestamp()) == null)
      return null;
    return cache.put(d.getTimestamp(), d);
  }
  
  public void deleteT(Long id) {
    if (cache.get(id) == null)
      return;
    cache.remove(id);
  }

  public ArrayList<T> getDataInRange(long start, long end) {
    ArrayList<T> range = new ArrayList<T>();
    SortedMap<Long, T> map;

    if (start > end)
      return range;
    
    try {
      map = cache.subMap(Long.valueOf(start), Long.valueOf(end + 1));
    } catch (NullPointerException e) {
      return range;
    }

    Iterator<T> iterator = map.values().iterator();
    while (iterator.hasNext()) {
      T d = iterator.next();
      if (d != null)
        range.add(d);
    }

    return range;
  }

  public ArrayList<T> getDataBefore(int number, long ms) {
    ArrayList<T> pre = new ArrayList<T>();
    SortedMap<Long, T> range;
    SortedMap<Long, T> reverse;
    
    try {
      range = cache.headMap(Long.valueOf(ms));
    } catch (NullPointerException e) {
      return pre;
    } catch (IllegalArgumentException e) {
      return pre;
    }
    
    reverse = new TreeMap<Long, T>(java.util.Collections.reverseOrder());
    reverse.putAll(range);

    Iterator<T> iterator = reverse.values().iterator();
    for (int i = 0; i < number && iterator.hasNext();) {
      T d = iterator.next();
      if (d != null) {
        i++;
        pre.add(0, d);
      }
    }

    return pre;
  }

  public ArrayList<T> getDataAfter(int number, long ms) {
    ArrayList<T> post = new ArrayList<T>();
    SortedMap<Long, T> range;

    try {
      range = cache.tailMap(Long.valueOf(ms) + 1);
    } catch (NullPointerException e) {
      return post;
    } catch (IllegalArgumentException e) {
      return post;
    }

    Iterator<T> iterator = range.values().iterator();
    for (int i = 0; i < number && iterator.hasNext();) {
      T d = iterator.next();
      if (d != null) {
        i++;
        post.add(d);
      }
    }

    return post;
  }

  public ArrayList<T> getLast(int number) {
    ArrayList<T> last = new ArrayList<T>();
    SortedMap<Long, T> reverse = new TreeMap<Long, T>(
        java.util.Collections.reverseOrder());
    reverse.putAll(cache);

    Iterator<T> iterator = reverse.values().iterator();
    for (int i = 0; i < number && iterator.hasNext();) {
      T d = iterator.next();
      if (d != null) {
        i++;
        last.add(0, d);
      }
    }

    return last;
  }

  private void resetRangeMarkers() {
    isValid = false;
    start = Long.MAX_VALUE;
    end = Long.MIN_VALUE;
  }
}

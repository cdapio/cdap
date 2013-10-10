package com.continuuity.data2.dataset.lib.table;

import com.google.common.annotations.GwtCompatible;
import com.google.common.annotations.GwtIncompatible;
import com.google.common.base.Objects;
import com.google.common.collect.ForwardingSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class is a copy of Guava's unmodifiableNavigableMap implementation.  The reason
 * it is copied here is that the method is only available in version 12.0 and later, whereas hadoop uses version 11.x.
 * This causes a conflict, and mapreduce jobs fail with a NoSuchMethodError.
 * Need to figure out a better solution, but replicating logic for now in order to get things working.
 */
public class MapsUtil {
  /**
   * Returns an unmodifiable view of the specified navigable map. Query operations on the returned
   * map read through to the specified map, and attempts to modify the returned map, whether direct
   * or via its views, result in an {@code UnsupportedOperationException}.
   *
   * <p>The returned navigable map will be serializable if the specified navigable map is
   * serializable.
   *
   * @param map the navigable map for which an unmodifiable view is to be returned
   * @return an unmodifiable view of the specified navigable map
   * @since 12.0
   */
  @GwtIncompatible("NavigableMap")
  public static <K, V> NavigableMap<K, V> unmodifiableNavigableMap(NavigableMap<K, V> map) {
    checkNotNull(map);
    if (map instanceof UnmodifiableNavigableMap) {
      return map;
    } else {
      return new UnmodifiableNavigableMap<K, V>(map);
    }
  }

  /**
   * Returns an unmodifiable view of the specified map entry. The {@link
   * java.util.Map.Entry#setValue} operation throws an {@link UnsupportedOperationException}.
   * This also has the side-effect of redefining {@code equals} to comply with
   * the Entry contract, to avoid a possible nefarious implementation of equals.
   *
   * @param entry the entry for which to return an unmodifiable view
   * @return an unmodifiable view of the entry
   */
  static <K, V> Map.Entry<K, V> unmodifiableEntry(final Map.Entry<K, V> entry) {
    checkNotNull(entry);
    return new AbstractMapEntry<K, V>() {
      @Override public K getKey() {
        return entry.getKey();
      }

      @Override public V getValue() {
        return entry.getValue();
      }
    };
  }

  @Nullable
  private static <K, V> Map.Entry<K, V> unmodifiableOrNull(@Nullable Map.Entry<K, V> entry) {
    return (entry == null) ? null : MapsUtil.unmodifiableEntry(entry);
  }

  @GwtIncompatible("NavigableMap")
  static class UnmodifiableNavigableMap<K, V>
    extends ForwardingSortedMap<K, V> implements NavigableMap<K, V>, Serializable {
    private final NavigableMap<K, V> delegate;

    UnmodifiableNavigableMap(NavigableMap<K, V> delegate) {
      this.delegate = delegate;
    }

    @Override
    protected SortedMap<K, V> delegate() {
      return Collections.unmodifiableSortedMap(delegate);
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
      return unmodifiableOrNull(delegate.lowerEntry(key));
    }

    @Override
    public K lowerKey(K key) {
      return delegate.lowerKey(key);
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
      return unmodifiableOrNull(delegate.floorEntry(key));
    }

    @Override
    public K floorKey(K key) {
      return delegate.floorKey(key);
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
      return unmodifiableOrNull(delegate.ceilingEntry(key));
    }

    @Override
    public K ceilingKey(K key) {
      return delegate.ceilingKey(key);
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
      return unmodifiableOrNull(delegate.higherEntry(key));
    }

    @Override
    public K higherKey(K key) {
      return delegate.higherKey(key);
    }

    @Override
    public Entry<K, V> firstEntry() {
      return unmodifiableOrNull(delegate.firstEntry());
    }

    @Override
    public Entry<K, V> lastEntry() {
      return unmodifiableOrNull(delegate.lastEntry());
    }

    @Override
    public final Entry<K, V> pollFirstEntry() {
      throw new UnsupportedOperationException();
    }

    @Override
    public final Entry<K, V> pollLastEntry() {
      throw new UnsupportedOperationException();
    }

    private transient UnmodifiableNavigableMap<K, V> descendingMap;

    @Override
    public NavigableMap<K, V> descendingMap() {
      UnmodifiableNavigableMap<K, V> result = descendingMap;
      if (result == null) {
        descendingMap = result = new UnmodifiableNavigableMap<K, V>(delegate.descendingMap());
        result.descendingMap = this;
      }
      return result;
    }

    @Override
    public Set<K> keySet() {
      return navigableKeySet();
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
      return Sets.unmodifiableNavigableSet(delegate.navigableKeySet());
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
      return Sets.unmodifiableNavigableSet(delegate.descendingKeySet());
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
      return subMap(fromKey, true, toKey, false);
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
      return headMap(toKey, false);
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
      return tailMap(fromKey, true);
    }

    @Override
    public
    NavigableMap<K, V>
    subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      return Maps.unmodifiableNavigableMap(delegate.subMap(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
      return Maps.unmodifiableNavigableMap(delegate.headMap(toKey, inclusive));
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
      return Maps.unmodifiableNavigableMap(delegate.tailMap(fromKey, inclusive));
    }
  }

  /**
   * Implementation of the {@code equals}, {@code hashCode}, and {@code toString}
   * methods of {@code Entry}.
   */
  @GwtCompatible
  abstract static class AbstractMapEntry<K, V> implements Map.Entry<K, V> {

    @Override
    public abstract K getKey();

    @Override
    public abstract V getValue();

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean equals(@Nullable Object object) {
      if (object instanceof Map.Entry) {
        Map.Entry<?, ?> that = (Map.Entry<?, ?>) object;
        return Objects.equal(this.getKey(), that.getKey())
          && Objects.equal(this.getValue(), that.getValue());
      }
      return false;
    }

    @Override public int hashCode() {
      K k = getKey();
      V v = getValue();
      return ((k == null) ? 0 : k.hashCode()) ^ ((v == null) ? 0 : v.hashCode());
    }

    /**
     * Returns a string representation of the form {@code {key}={value}}.
     */
    @Override public String toString() {
      return getKey() + "=" + getValue();
    }
  }

}

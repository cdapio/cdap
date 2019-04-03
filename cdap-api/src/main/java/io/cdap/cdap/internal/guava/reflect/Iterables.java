/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.guava.reflect;

import co.cask.cdap.api.Predicate;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class contains static utility methods that operate on or return objects
 * of type {@code Iterable}.
 */
final class Iterables {

  static <F, T> Iterable<T> transform(final Iterable<F> iterable, final Function<? super F, ? extends T> transform) {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        final Iterator<F> itor = iterable.iterator();
        return new Iterator<T>() {
          @Override
          public boolean hasNext() {
            return itor.hasNext();
          }

          @Override
          public T next() {
            return transform.apply(itor.next());
          }

          @Override
          public void remove() {
            itor.remove();
          }
        };
      }
    };
  }

  static <T, C> Iterable<C> filter(Iterable<T> iterable, final Class<C> cls) {
    @SuppressWarnings("unchecked")
    Iterable<C> result = (Iterable<C>) filter(iterable, new Predicate<T>() {
      @Override
      public boolean apply(T input) {
        return cls.isInstance(input);
      }
    });
    return result;
  }

  static <T> Iterable<T> filter(final Iterable<T> iterable, final Predicate<? super T> predicate) {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        final Iterator<T> itor = iterable.iterator();
        return new Iterator<T>() {

          private boolean hasNext = false;
          private T next = null;

          @Override
          public boolean hasNext() {
            while (!hasNext) {
              boolean sourceHasNext = itor.hasNext();
              if (!sourceHasNext) {
                break;
              }
              T sourceNext = itor.next();
              if (predicate.apply(sourceNext)) {
                hasNext = true;
                next = sourceNext;
                break;
              }
            }
            return hasNext;
          }

          @Override
          public T next() {
            if (hasNext()) {
              hasNext = false;
              T result = next;
              next = null;
              return result;
            }
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  static <E, C extends Collection<? super E>> C addAll(Iterable<E> iterable, C collection) {
    for (E element : iterable) {
      collection.add(element);
    }
    return collection;
  }

  private Iterables() {
    // no-op
  }
}

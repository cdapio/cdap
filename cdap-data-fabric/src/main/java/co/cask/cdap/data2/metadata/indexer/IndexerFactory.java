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

package co.cask.cdap.data2.metadata.indexer;

import com.google.common.base.Throwables;

/**
 * A factory for {@link Indexer}
 */
public class IndexerFactory {

  /**
   * Gets the {@link Indexer} for an a given {@link IndexerType}
   *
   * @param indexerType the type of {@link Indexer} needed
   * @return the {@link Indexer} for the given {@link IndexerType}
   */
  public static Indexer getIndexer(IndexerType indexerType) {
    try {
      return getIndexer(indexerType.getIndexerClass());
    } catch (IllegalAccessException | InstantiationException e) {
      throw Throwables.propagate(e);
    }
  }

  private static Indexer getIndexer(Class<? extends Indexer> indexer)
    throws IllegalAccessException, InstantiationException {
    return indexer.newInstance();
  }
}

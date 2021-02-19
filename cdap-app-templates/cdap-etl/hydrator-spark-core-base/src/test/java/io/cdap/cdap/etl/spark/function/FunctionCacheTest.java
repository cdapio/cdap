/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.function;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Unit tests for {@link FunctionCache}
 */
public class FunctionCacheTest {
  @Test
  public void testEnabledCache() throws Exception {
    FunctionCache.Factory factory = FunctionCache.Factory.newInstance(true);
    FunctionCache cache1 = factory.newCache();
    FunctionCache cache2 = factory.newCache();
    FunctionCache movedCache1Copy1 = SerializationUtils.clone(cache1);
    FunctionCache movedCache1Copy2 = SerializationUtils.clone(cache1);
    FunctionCache movedCache2Copy1 = SerializationUtils.clone(cache2);

    Callable<Object> throwingLoader = () -> {
      throw new IllegalStateException("Must use cached value");
    };

    Assert.assertEquals("Test", movedCache1Copy1.getValue(() -> "Test"));
    Assert.assertEquals("Test", movedCache1Copy1.getValue(throwingLoader));
    Assert.assertEquals("Test", movedCache1Copy2.getValue(throwingLoader));
    Assert.assertEquals("Test", cache1.getValue(throwingLoader));

    Assert.assertEquals("OtherValue", movedCache2Copy1.getValue(() -> "OtherValue"));
    Assert.assertEquals("OtherThreadValue",
                        CompletableFuture.supplyAsync(() -> {
                          try {
                            return movedCache1Copy1.getValue(() -> "OtherThreadValue");
                          } catch (Exception e) {
                            throw new IllegalStateException(e);
                          }
                        }).get());
  }

  @Test
  public void testDisabledCache() throws Exception {
    FunctionCache.Factory factory = FunctionCache.Factory.newInstance(false);
    FunctionCache cache1 = factory.newCache();
    FunctionCache movedCache1Copy1 = SerializationUtils.clone(cache1);
    FunctionCache movedCache1Copy2 = SerializationUtils.clone(cache1);
    Assert.assertEquals("Test", movedCache1Copy1.getValue(() -> "Test"));
    Assert.assertEquals("Test2", movedCache1Copy1.getValue(() -> "Test2"));
    Assert.assertEquals("Test3", movedCache1Copy2.getValue(() -> "Test3"));
  }
}

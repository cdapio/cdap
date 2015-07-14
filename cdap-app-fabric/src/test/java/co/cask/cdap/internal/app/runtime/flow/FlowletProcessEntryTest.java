/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.annotation.Tick;
import org.junit.Assert;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link FlowletProcessEntry}.
 */
public class FlowletProcessEntryTest {

  @Test
  public void testInitialDelay() {
    long nanoTimeStart = System.nanoTime();
    FlowletProcessEntry entry = FlowletProcessEntry.create(
      new ProcessSpecification<>(null, null, new TickObject(100)));
    Assert.assertEquals(floorNanosToSec(nanoTimeStart + TimeUnit.SECONDS.toNanos(100)),
                        floorNanosToSec(entry.getNextDeque()));
  }

  @Test
  public void testInitialDelayOverflow() {
    FlowletProcessEntry entry = FlowletProcessEntry.create(
      new ProcessSpecification<>(null, null, new TickObject(Long.MAX_VALUE)));
    Assert.assertEquals(Long.MAX_VALUE, entry.getNextDeque());
  }

  private long floorNanosToSec(long nanoseconds) {
    return TimeUnit.SECONDS.toNanos(TimeUnit.NANOSECONDS.toSeconds(nanoseconds));
  }

  /**
   * Convenience class to construct {@link Tick} with a specified initialDelay.
   */
  private static class TickObject implements Tick {

    private final long initialDelaySec;

    private TickObject(long initialDelaySec) {
      this.initialDelaySec = initialDelaySec;
    }

    @Override
    public long initialDelay() {
      return initialDelaySec;
    }

    @Override
    public long delay() {
      return 0;
    }

    @Override
    public TimeUnit unit() {
      return TimeUnit.SECONDS;
    }

    @Override
    public int maxRetries() {
      return Integer.MAX_VALUE;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
      return Tick.class;
    }
  }
}

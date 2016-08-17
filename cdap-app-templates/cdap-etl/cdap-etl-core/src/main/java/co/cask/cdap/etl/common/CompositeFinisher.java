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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.MultiInputBatchConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A finisher of one or more {@link BatchConfigurable BatchConfigurables}.
 * If any of them throws an exception, the exception will be logged before moving on to finish the other objects.
 */
public class CompositeFinisher implements Finisher {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeFinisher.class);
  private final List<Finisher> finishers;

  private CompositeFinisher(List<Finisher> finishers) {
    this.finishers = finishers;
  }

  /**
   * Run logic on program finish.
   *
   * @param succeeded whether the program run succeeded or not
   */
  public void onFinish(boolean succeeded) {
    for (Finisher finisher : finishers) {
      finisher.onFinish(succeeded);
    }
  }

  /**
   * @return builder for creating a composite finisher.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to create a composite finisher
   */
  public static class Builder {
    private final List<Finisher> finishers;

    public Builder() {
      this.finishers = new ArrayList<>();
    }

    public <T extends BatchContext> Builder add(final BatchConfigurable<T> configurable, final T context) {
      finishers.add(new Finisher() {
        @Override
        public void onFinish(boolean succeeded) {
          try {
            configurable.onRunFinish(succeeded, context);
          } catch (Throwable t) {
            LOG.warn("Error calling onRunFinish on stage {}.", context.getStageName(), t);
          }
        }
      });
      return this;
    }

    public <T extends BatchContext> Builder add(final MultiInputBatchConfigurable<T> configurable, final T context) {
      finishers.add(new Finisher() {
        @Override
        public void onFinish(boolean succeeded) {
          try {
            configurable.onRunFinish(succeeded, context);
          } catch (Throwable t) {
            LOG.warn("Error calling onRunFinish on stage {}.", context.getStageName(), t);
          }
        }
      });
      return this;
    }

    public CompositeFinisher build() {
      return new CompositeFinisher(finishers);
    }
  }
}

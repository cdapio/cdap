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

package co.cask.cdap;

import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.base.Objects;

/**
 * App Template to test adapter lifecycle.
 */
public class DummyTemplate extends ApplicationTemplate<DummyTemplate.Config> {
  public static final String NAME = "DummyTemplate";

  public static class Config {
    private final String field1;
    private final String field2;

    public Config(String field1, String field2) {
      this.field1 = field1;
      this.field2 = field2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Config that = (Config) o;

      return Objects.equal(field1, that.field1) && Objects.equal(field2, that.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(field1, field2);
    }
  }

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application for to test Adapter lifecycle");
    addWorkflow(new AdapterWorkflow());
    addMapReduce(new DummyMapReduceJob());
  }

  /**
   *
   */
  public static class AdapterWorkflow extends AbstractWorkflow {
    public static final String NAME = "AdapterWorkflow";
    @Override
    protected void configure() {
      setName(NAME);
      setDescription("Workflow to test Adapter");
      addMapReduce(DummyMapReduceJob.NAME);
    }
  }

  /**
   *
   */
  public static class DummyMapReduceJob extends AbstractMapReduce {
    public static final String NAME = "DummyMapReduceJob";
    @Override
    protected void configure() {
      setName(NAME);
      setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here to test Adapter lifecycle");
    }
  }
}

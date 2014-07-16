/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.pipeline;

import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests simple pipelining
 */
public class PipelineTest {

  /**
   *
   */
  public static final class HowStage extends AbstractStage<String> {
    public HowStage() {
      super(TypeToken.of(String.class));
    }

    @Override
    public void process(String msg) {
        msg += ", how ";
        emit(msg);
    }
  }

  /**
   *
   */
  public static class AreStage extends AbstractStage<String> {
    public AreStage() {
      super(TypeToken.of(String.class));
    }

    @Override
    public void process(String msg) {
      msg += " are ";
      emit(msg);
    }
  }

  /**
   *
   */
  public static class YouStage extends AbstractStage<String> {
    public YouStage() {
      super(TypeToken.of(String.class));
    }

    @Override
    public void process(String msg) {
      msg += " you";
      emit(msg);
    }
  }

  @Test
  public void testSimplePipeline() throws Exception {
    PipelineFactory factory = new SynchronousPipelineFactory();
    Pipeline<String> pipeline = factory.getPipeline();
    pipeline.addLast(new HowStage());
    pipeline.addLast(new AreStage());
    pipeline.addLast(new YouStage());
    String s = pipeline.execute("Hi").get();
    Assert.assertTrue(s.equals("Hi, how  are  you"));
  }



}

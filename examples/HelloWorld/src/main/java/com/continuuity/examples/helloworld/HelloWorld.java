/*
 * Copyright 2014 Continuuity, Inc.
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
package com.continuuity.examples.helloworld;

import com.continuuity.api.app.AbstractApplication;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple HelloWorld example that uses one stream, one dataset, one flow and one procedure.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A procedure that reads the name from the KeyValueTable and prints 'Hello [Name]!'</li>
 * </uL>
 */
public class HelloWorld extends AbstractApplication {

  @Override
  public void configure() {
    setName("HelloWorld");
    setDescription("A Hello World program for the Continuuity Reactor");
    addService("HelloWorldService", new HelloWorldService());
  }

  /**
   * Test
   */
  public static final class HelloWorldService extends AbstractIdleService {
    private static final Logger LOG = LoggerFactory.getLogger(HelloWorldService.class);
    @Override
    protected void startUp() throws Exception {
      LOG.info("!!!!!!! Starting Guava Service");
    }

    @Override
    protected void shutDown() throws Exception {
      LOG.info("!!!!!!! Stopping Guava Service");
    }
  }


}


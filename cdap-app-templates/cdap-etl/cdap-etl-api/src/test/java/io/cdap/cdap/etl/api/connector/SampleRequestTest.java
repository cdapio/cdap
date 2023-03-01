/*
 * Copyright © 2022 Cask Data, Inc.
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


package io.cdap.cdap.etl.api.connector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SampleRequest} and related builder.
 */
public class SampleRequestTest {
  private static final String PATH = "/database/table";
  private static final int LIMIT = 1000;
  private static final Map<String, String> PROPERTIES = new HashMap<String, String>() {
    {
      put("sampleType", "stratified");
      put("strata", "strata_col");
    }
  };
  private static final Long TIMEOUTMS = 10000L;

  @Test
  public void testConstructor() {
    SampleRequest sampleRequest = new SampleRequest(PATH, LIMIT, PROPERTIES, TIMEOUTMS);

    Assert.assertEquals(PATH, sampleRequest.getPath());
    Assert.assertEquals(LIMIT, sampleRequest.getLimit());
    Assert.assertEquals(PROPERTIES, sampleRequest.getProperties());
    Assert.assertEquals(TIMEOUTMS, sampleRequest.getTimeoutMs());
  }

  @Test
  public void testConstructorNoTimeout() {
    SampleRequest sampleRequest = new SampleRequest(PATH, LIMIT, PROPERTIES, null);
    Assert.assertNull(sampleRequest.getTimeoutMs());
  }

  @Test
  public void testConstructorNoProperties() {
    SampleRequest sampleRequest = new SampleRequest(PATH, LIMIT, null, TIMEOUTMS);
    Assert.assertEquals(Collections.emptyMap(), sampleRequest.getProperties());
  }

  @Test
  public void testBuilder() {
    SampleRequest sampleRequest = new SampleRequest.Builder(LIMIT)
      .setPath(PATH)
      .setLimit(LIMIT)
      .setProperties(PROPERTIES)
      .build();

    Assert.assertEquals(PATH, sampleRequest.getPath());
    Assert.assertEquals(LIMIT, sampleRequest.getLimit());
    Assert.assertEquals(PROPERTIES, sampleRequest.getProperties());
    // Timeout is only retrieved from JSON, so there is no need for the builder to set it
    Assert.assertNull(sampleRequest.getTimeoutMs());
  }
}

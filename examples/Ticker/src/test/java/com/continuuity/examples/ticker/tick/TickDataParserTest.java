/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.ticker.tick;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;


/**
 *
 */
public class TickDataParserTest {

  @Test
  public void testIterator() {
    StringBuilder builder = new StringBuilder();
    builder.append("20131023T153000\t500.0\t100\n");
    builder.append("20131023T153000\t500.03\t500\n");
    builder.append("20131023T153001\t499.99\t8\n");
    String symbol = "AAPL";
    Exchange exchange = Exchange.NASDAQ;
    BufferedReader breader = new BufferedReader(new StringReader(builder.toString()));
    TickDataParser.TickDataIterator iter = new TickDataParser.TickDataIterator(breader, symbol, exchange);

    TickerDataPoint expected = new TickerDataPoint(symbol, exchange, 1382535000, 100, 500.0f);
    Assert.assertEquals(expected, iter.next());

    expected = new TickerDataPoint(symbol, exchange, 1382535000, 500, 500.03f);
    Assert.assertEquals(expected, iter.next());

    expected = new TickerDataPoint(symbol, exchange, 1382535001, 8, 499.99f);
    Assert.assertEquals(expected, iter.next());
    Assert.assertFalse(iter.hasNext());
  }
}

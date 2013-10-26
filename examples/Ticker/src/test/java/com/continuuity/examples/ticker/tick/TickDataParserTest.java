/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.examples.ticker.tick;

import junit.framework.Assert;
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

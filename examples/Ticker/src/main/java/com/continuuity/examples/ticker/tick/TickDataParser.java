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

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.examples.ticker.TimeUtil;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.CharacterCodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Receives requests to parse a specific ticker symbol for some time range, sends out an http
 * request to pull data for that symbol and time range, aggregates trades that occurred in the
 * same second, then sends the data for each second on to the saver to store in a dataset.
 * Will break if the data source changes its format.  Most of the work is done in this flowlet, so it
 * is a candidate for increases instance count if the flow is too slow.
 */
public class TickDataParser extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TickDataParser.class);
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

  private OutputEmitter<TickerDataSecond> output;

  @ProcessInput
  public void process(TickerRequest tickerReq) throws CharacterCodingException {
    String ticker = tickerReq.getTicker();
    Exchange exchange = Exchange.NASDAQ;
    long startTs = tickerReq.getStartTs();
    long endTs = tickerReq.getEndTs();
    String startDate = tsToDateStr(tickerReq.getStartTs());
    String endDate = tsToDateStr(tickerReq.getEndTs());

    emitDataForDate(ticker, exchange, startDate, startTs, endTs);
    if (!startDate.equals(endDate)) {
      emitDataForDate(ticker, exchange, endDate, startTs, endTs);
    }
  }

  private void emitDataForDate(String ticker, Exchange exchange, String date, long startTs, long endTs) {
    TickerDataSecond dataSecond = new TickerDataSecond(ticker, exchange, 0);
    TickDataReader tickReader = new TickDataReader(ticker, exchange, date);
    for (TickerDataPoint currPoint : tickReader) {
      
      // Skip earlier points.
      if (currPoint.getTimestamp() < startTs) {
        continue;
      }
      // Finished if we're past the end
      if (currPoint.getTimestamp() > endTs) {
        break;
      }

      // If we're now on a new second
      if (dataSecond.getTimestamp() != currPoint.getTimestamp()) {
        if (dataSecond.getQuantity() > 0) {
          output.emit(dataSecond);
        }
        dataSecond = new TickerDataSecond(ticker, exchange, currPoint.getTimestamp());
      }

      // Aggregate all data that happened within the same second
      dataSecond.addPoint(currPoint);
    }
    if (dataSecond.getQuantity() > 0) {
      output.emit(dataSecond);
    }
    tickReader.close();
  }

  private String tsToDateStr(long ts) {
    return dateFormat.format(new Date(1000 * ts)).toString();
  }

  /**
   * Tick data reader.
   */
  public class TickDataReader implements Iterable<TickerDataPoint> {
    private BufferedReader reader;
    private final String ticker;
    private final Exchange exchange;
    private HttpURLConnection conn;

    public TickDataReader(String ticker, Exchange exchange, String date) {
      this.ticker = ticker;
      this.exchange = exchange;

      try {
        String paper = ticker + "." + exchange.getCode();
        
        // Get URL content
        URL url = new URL("http://hopey.netfonds.no/tradedump.php?date=" + date + "&paper="
                            + paper + "&csv_format=txt");
        LOG.debug("making request to {}", url.toString());
        conn = (HttpURLConnection) url.openConnection();

        // Open the stream and put it into BufferedReader
        reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), Charsets.UTF_8));
        
        // Ignore first line since it contains the field names
        reader.readLine();
      } catch (MalformedURLException e) {
        LOG.error("malformed url while trying to get tick data for ticker {}", ticker, e);
      } catch (IOException e) {
        LOG.error("IOException while trying to get tick data for ticker {}", ticker, e);
      }
    }

    @Override
    public Iterator<TickerDataPoint> iterator() {
      return new TickDataIterator(reader, ticker, exchange);
    }

    public void close() {
      conn.disconnect();
      try {
        reader.close();
      } catch (IOException e) {
        LOG.error("unable to close reader", e);
      }
    }
  }

  static class TickDataIterator implements Iterator<TickerDataPoint> {
    private final BufferedReader reader;
    private final String ticker;
    private final Exchange exchange;
    private TickerDataPoint currentTick;

    public TickDataIterator(BufferedReader reader, String ticker, Exchange exchange) {
      this.reader = reader;
      this.ticker = ticker;
      this.exchange = exchange;
      this.currentTick = null;
    }

    @Override
    public boolean hasNext() {
      try {
        if (currentTick != null) {
          return true;
        } else {
          String line = reader.readLine();
          if (line != null) {
            String[] fields = line.split("\t");
            long timestamp = TimeUtil.netfondsDateToTimestamp(fields[0]);
            float price = Float.parseFloat(fields[1]);
            int quantity = Integer.parseInt(fields[2]);
            currentTick = new TickerDataPoint(ticker, exchange, timestamp, quantity, price);
            return true;
          } else {
            return false;
          }
        }
      } catch (IOException e) {
        return false;
      } catch (ParseException e) {
        return false;
      }
    }

    @Override
    public TickerDataPoint next() {
      if (hasNext()) {
        TickerDataPoint out = currentTick;
        currentTick = null;
        return out;
      } else {
        throw new NoSuchElementException("no more elements in iterator");
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not supported");
    }
  }
}

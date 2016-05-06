/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import java.io.UnsupportedEncodingException;

/**
 * This is a sample Web Crawler application that is used is test.
 * <p>
 *   This Application has multiple flowlets
 *   <ul>
 *     <li>Document Crawler : Gets urls from stream and crawls the pages and stores them. </li>
 *   </ul>
 * </p>
 */
public class WebCrawlApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("WebCrawlerApp");
    setDescription("Web Crawler Application");
    addStream(new Stream("urls"));
    createDataset("crawled-pages", KeyValueTable.class);
    addFlow(new CrawlFlow());
  }

  /**
   * Defines a document url.
   */
  public interface DocumentURL {
    String getURL();
  }

  /**
   *
   */
  public static final class DocumentURLImpl implements DocumentURL {
    private final String url;

    public DocumentURLImpl(String url) {
      this.url = url;
    }

    @Override
    public String getURL() {
      return url;
    }
  }

  /**
   * Defines a basic document.
   */
  public interface Document {
    String getBody();
    String getMeta();
    String getResolveInfo();
    Long   getLoadTime();
  }

  /**
   *
   */
  public static final class DocumentImpl implements Document {
    private final String body;
    private final String meta;
    private final String resolveInfo;
    private final Long loadTime;

    public DocumentImpl(String body, String meta, String resolveInfo, Long loadTime) {
      this.body = body;
      this.meta = meta;
      this.resolveInfo = resolveInfo;
      this.loadTime = loadTime;
    }

    @Override
    public String getBody() {
      return body;
    }

    @Override
    public String getMeta() {
      return meta;
    }

    @Override
    public String getResolveInfo() {
      return resolveInfo;
    }

    @Override
    public Long getLoadTime() {
      return loadTime;
    }
  }

  /**
   * Simple part of crawler that is responsible for retrieving URLs
   * coming on the stream put there by a scheduler and being crawled
   * and the content is stored in KV Table.
   */
  public static final class CrawlFlow extends AbstractFlow {

    @Override
    protected void configure() {
      setName("CrawlFlow");
      setDescription("Flow for crawling pages");
      addFlowlet(new UrlSanitizer());
      addFlowlet(new UrlCrawler());
      connectStream("url", new UrlSanitizer());
      connect(new UrlSanitizer(), new UrlCrawler());
    }
  }

  /**
   * Sanitizes the url.
   */
  public static final class UrlSanitizer extends AbstractFlowlet {
    private OutputEmitter<DocumentURL> output;

    @ProcessInput
    public void process(StreamEvent event) {
      // Does some-fancy sanitization of url.
      output.emit(new DocumentURLImpl(event.getHeaders().get("url")));
    }

    @Override
    protected void configure() {
      setName("UrlSanitizer");
    }
  }

  /**
   * Crawls the page using the url. Given a sanitized document url
   */
  public static final class UrlCrawler extends AbstractFlowlet {
    @UseDataSet("crawled-pages")
    private KeyValueTable crawledPages;

    @ProcessInput
    public void process(DocumentURL url) throws UnsupportedEncodingException {
      // ... does some fancy crawling
      // Marks that the url has been crawled.
      crawledPages.write(url.getURL().getBytes("UTF8"), "crawled".getBytes("UTF8"));
    }

    @Override
    protected void configure() {
      setName("UrlCrawler");
    }
  }
}

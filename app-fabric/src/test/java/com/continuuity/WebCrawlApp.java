/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

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
public class WebCrawlApp implements Application {
  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("WebCrawlerApp")
      .setDescription("Web Crawler Application")
      .withStreams().add(new Stream("urls"))
      .withDataSets().add(new KeyValueTable("crawled-pages"))
      .withFlows().add(new CrawlFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   * Defines a document url.
   */
  public static interface DocumentURL {
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
  public static interface Document {
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
  public static final class CrawlFlow implements Flow {
    /**
     * Configure the {@link com.continuuity.api.flow.Flow} by returning an
     * {@link com.continuuity.api.flow.FlowSpecification}.
     *
     * @return An instance of {@link com.continuuity.api.flow.FlowSpecification}.
     */
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("CrawlFlow")
        .setDescription("Flow for crawling pages")
        .withFlowlets().add(new UrlSanitizer())
        .add(new UrlCrawler())
        .connect().from(new Stream("url")).to(new UrlSanitizer())
        .from(new UrlSanitizer()).to(new UrlCrawler())
        .build();
      }
  }

  /**
   * Sanitizes the url.
   */
  public static final class UrlSanitizer extends AbstractFlowlet {
    private OutputEmitter<DocumentURL> output;

    public UrlSanitizer() {
      super("UrlSanitizer");
    }

    @ProcessInput
    public void process(StreamEvent event) {
      // Does some-fancy sanitization of url.
      output.emit(new DocumentURLImpl(event.getHeaders().get("url")));
    }
  }

  /**
   * Crawls the page using the url. Given a sanitized document url
   */
  public static final class UrlCrawler extends AbstractFlowlet {
    @UseDataSet("crawled-pages")
    private KeyValueTable crawledPages;

    public UrlCrawler() {
      super("UrlCrawler");
    }

    @ProcessInput
    public void process(DocumentURL url) throws UnsupportedEncodingException {
      // ... does some fancy crawling
      // Marks that the url has been crawled.
      crawledPages.write(url.getURL().getBytes("UTF8"), "crawled".getBytes("UTF8"));
    }
  }


}

package com.continuuity.gateway.router;

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test header decoder.
 */
public class HeaderDecoderTest {

  @Test
  public void testHeaderDecode1() throws Exception {
    String message =
      "GET / HTTP/1.1\r\n" +
      "Host: www.yahoo.com\r\n" +
      "Connection: close\r\n" +
      "User-Agent: Web-sniffer/1.0.46 (+http://web-sniffer.net/)\r\n" +
      "Accept-Encoding: gzip\r\n" +
      "Accept-Charset:     ISO-8859-1,UTF-8;q=0.7,*;q=0.7   \r\n" +
      "Cache-Control: no-cache\r\n" +
      "Accept-Language: de,en;q=0.7,en-us;q=0.3\r\n" +
      "Referer: http://web-sniffer.net/\r\n" +
      "\r\n" +
      "Message-body: message body\r\n";

    HeaderDecoder.HeaderInfo headerInfo =
      HeaderDecoder.decodeHeader(ChannelBuffers.wrappedBuffer(message.getBytes(Charsets.UTF_8)));
    Assert.assertEquals("/", headerInfo.getPath());
    Assert.assertEquals("www.yahoo.com", headerInfo.getHost());
  }

  @Test
  public void testHeaderDecode2() throws Exception {
    String message =
      "GET http://www.yahoo.com:9876/index.html HTTP/1.1\r\n" +
        "Connection: close\r\n" +
        "User-Agent: Web-sniffer/1.0.46 (+http://web-sniffer.net/)\r\n" +
        "Accept-Encoding: gzip\r\n" +
        "Accept-Charset:     ISO-8859-1,UTF-8;q=0.7,*;q=0.7   \r\n" +
        "Cache-Control: no-cache\r\n" +
        "Accept-Language: de,en;q=0.7,en-us;q=0.3\r\n" +
        "Referer: http://web-sniffer.net/\r\n" +
        "Host:    www.yahoo.com   \r\n" +
        "\r\n" +
        "Message-body: message body\r\n";

    HeaderDecoder.HeaderInfo headerInfo =
      HeaderDecoder.decodeHeader(ChannelBuffers.wrappedBuffer(message.getBytes(Charsets.UTF_8)));
    Assert.assertEquals("/index.html", headerInfo.getPath());
    Assert.assertEquals("www.yahoo.com", headerInfo.getHost());
  }

  @Test
  public void testHeaderDecode3() throws Exception {
    String message =
      "GET http://www.yahoo.com:9876/ HTTP/1.1\r\n" +
        "Connection: close\r\n" +
        "User-Agent: Web-sniffer/1.0.46 (+http://web-sniffer.net/)\r\n" +
        "Accept-Encoding: gzip\r\n" +
        "Accept-Charset:     ISO-8859-1,UTF-8;q=0.7,*;q=0.7   \r\n" +
        "Cache-Control: no-cache\r\n" +
        "Accept-Language: de,en;q=0.7,en-us;q=0.3\r\n" +
        "Referer: http://web-sniffer.net/\r\n" +
        "Host:    www.yahoo.com   \r\n" +
        "\r\n" +
        "Message-body: message body\r\n";

    HeaderDecoder.HeaderInfo headerInfo =
      HeaderDecoder.decodeHeader(ChannelBuffers.wrappedBuffer(message.getBytes(Charsets.UTF_8)));
    Assert.assertEquals("/", headerInfo.getPath());
    Assert.assertEquals("www.yahoo.com", headerInfo.getHost());
  }

  @Test
  public void testNoHeader() throws Exception {
    String message =
      "GET / HTTP/1.1\r\n" +
        "Connection: close\r\n" +
        "User-Agent: Web-sniffer/1.0.46 (+http://web-sniffer.net/)\r\n" +
        "Accept-Encoding: gzip\r\n" +
        "Accept-Charset:     ISO-8859-1,UTF-8;q=0.7,*;q=0.7   \r\n" +
        "Cache-Control: no-cache\r\n" +
        "Accept-Language: de,en;q=0.7,en-us;q=0.3\r\n" +
        "Referer: http://web-sniffer.net/\r\n" +
        "\r\n" +
        "Message-body: message body\r\n";

    HeaderDecoder.HeaderInfo headerInfo =
      HeaderDecoder.decodeHeader(ChannelBuffers.wrappedBuffer(message.getBytes(Charsets.UTF_8)));
    Assert.assertNull(headerInfo);
  }

  @Test
  public void testEmptyBody() throws Exception {
    String message =
      "GET /index.html HTTP/1.1\r\n" +
        "Host:     www.yahoo.com\r\n" +
        "Connection: close\r\n" +
        "User-Agent: Web-sniffer/1.0.46 (+http://web-sniffer.net/)\r\n" +
        "Accept-Encoding: gzip\r\n" +
        "Accept-Charset:     ISO-8859-1,UTF-8;q=0.7,*;q=0.7   \r\n" +
        "Cache-Control: no-cache\r\n" +
        "Accept-Language: de,en;q=0.7,en-us;q=0.3\r\n" +
        "Referer: http://web-sniffer.net/\r\n" +
        "\r\n";

    HeaderDecoder.HeaderInfo headerInfo =
      HeaderDecoder.decodeHeader(ChannelBuffers.wrappedBuffer(message.getBytes(Charsets.UTF_8)));
    Assert.assertEquals("/index.html", headerInfo.getPath());
    Assert.assertEquals("www.yahoo.com", headerInfo.getHost());
  }
}

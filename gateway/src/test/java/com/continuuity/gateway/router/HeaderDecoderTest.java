package com.continuuity.gateway.router;

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test header decoder.
 */
public class HeaderDecoderTest {

  @Test
  public void testHeaderDecode() throws Exception {
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

    testDecode(message);
  }

  @Test
  public void testEmptyBody() throws Exception {
    String message =
      "GET /index.html HTTP/1.1\r\n" +
        "Host: www.yahoo.com\r\n" +
        "Connection: close\r\n" +
        "User-Agent: Web-sniffer/1.0.46 (+http://web-sniffer.net/)\r\n" +
        "Accept-Encoding: gzip\r\n" +
        "Accept-Charset:     ISO-8859-1,UTF-8;q=0.7,*;q=0.7   \r\n" +
        "Cache-Control: no-cache\r\n" +
        "Accept-Language: de,en;q=0.7,en-us;q=0.3\r\n" +
        "Referer: http://web-sniffer.net/\r\n" +
        "\r\n";

    testDecode(message);
  }

  private void testDecode(String message) throws Exception {
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(message.getBytes(Charsets.US_ASCII));
    buffer.markReaderIndex();

    String headerValue = HeaderDecoder.decodeHeader(buffer, "Host");
    Assert.assertEquals("www.yahoo.com", headerValue);

    buffer.resetReaderIndex();
    headerValue = HeaderDecoder.decodeHeader(buffer, "Connection");
    Assert.assertEquals("close", headerValue);

    buffer.resetReaderIndex();
    headerValue = HeaderDecoder.decodeHeader(buffer, "Accept-Charset");
    Assert.assertEquals("ISO-8859-1,UTF-8;q=0.7,*;q=0.7", headerValue);

    buffer.resetReaderIndex();
    headerValue = HeaderDecoder.decodeHeader(buffer, "aCcePt-Language");
    Assert.assertEquals("de,en;q=0.7,en-us;q=0.3", headerValue);

    buffer.resetReaderIndex();
    headerValue = HeaderDecoder.decodeHeader(buffer, "User-Agent");
    Assert.assertEquals("Web-sniffer/1.0.46 (+http://web-sniffer.net/)", headerValue);

    // Unknown header
    buffer.resetReaderIndex();
    headerValue = HeaderDecoder.decodeHeader(buffer, "Date");
    Assert.assertNull(headerValue);

    // Message body
    buffer.resetReaderIndex();
    headerValue = HeaderDecoder.decodeHeader(buffer, "Message-body");
    Assert.assertNull(headerValue);
  }
}

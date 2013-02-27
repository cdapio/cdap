package com.continuuity.passport.utils;

import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 */
public class TestURI {

  @Test
  public void testUri() throws URISyntaxException, MalformedURLException {
    URI uri  = new URI ("http://foo.bar.com");
    URI appendedUri = getAppendedURI(uri,"/path/to/baz");

    assert(appendedUri.toURL().toString().equals("http://foo.bar.com/path/to/baz"));

  }

  @Test
  public void testUriWithQueryParam() throws URISyntaxException, MalformedURLException {
    URI uri  = new URI ("http://foo.bar.com?query=param");
    URI appendedUri =  getAppendedURI(uri, "query=param");
    assert(appendedUri.toURL().toString().equals("http://foo.bar.com/path/to/baz?query=param"));
  }

  public URI getAppendedURI(URI uri, String relativePath) throws MalformedURLException, URISyntaxException {
    if (uri.getQuery() !=null && uri.getQuery().isEmpty()){
      return new URI(String.format("%s/%s",uri.toURL(),relativePath));
    }
    else {
      return new URI(String.format("%s/%s?%s",uri.toURL(),relativePath,uri.getQuery()));
    }
  }
}

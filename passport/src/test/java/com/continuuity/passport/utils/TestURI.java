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
    URI appendedUri = getAppendedURI(uri, "path/to/baz");
    //URI(String scheme, String userInfo, String host, int port, String path, String query, String fragment)

    System.out.println(appendedUri.toURL().toString());
    assert(appendedUri.toURL().toString().equals("http://foo.bar.com/path/to/baz"));
  }

  @Test
  public void testUriWithQueryParam() throws URISyntaxException, MalformedURLException {
    URI uri  = new URI ("http://foo.bar.com?query=param");
    URI appendedUri =  getAppendedURI(uri, "path/to/baz");
    System.out.println(appendedUri.toURL().toString());
    assert(appendedUri.toURL().toString().equals("http://foo.bar.com/path/to/baz?query=param"));
  }

  @Test
  public void testUriWithQueryParamAndPort() throws URISyntaxException, MalformedURLException {
    URI uri  = new URI ("http://foo.bar.com:7777/bar?query=param");
    URI appendedUri =  getAppendedURI(uri, "path/to/baz");
    System.out.println(appendedUri.toURL().toString());
    assert(appendedUri.toURL().toString().equals("http://foo.bar.com:7777/bar/path/to/baz?query=param"));
  }


  public URI getAppendedURI(URI uri, String relativePath) throws MalformedURLException, URISyntaxException {
    return new URI (uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                    uri.getPath() + "/" + relativePath, uri.getQuery(), uri.getFragment());
  }
}

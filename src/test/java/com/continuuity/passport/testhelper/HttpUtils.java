package com.continuuity.passport.testhelper;

/**
 *
 */
public class HttpUtils {

//  private String httpGet(String baseUri, String api)  {
//    URI uri = URI.create( baseUri + "/" + api);
//    HttpGet get = new HttpGet(uri);
//    return request(get);
//  }
//
//  private String httpPost(String baseURI, String api) {
//    URI uri = URI.create(baseURI + "/" + api);
//    HttpPost post = new HttpPost(uri);
//    post.addHeader("Content-Type","application/json");
//    return request(post);
//  }
//
//  private String request(HttpUriRequest uri)  {
//    LOG.trace("Requesting " + uri.getURI().toASCIIString());
//    HttpClient client = new DefaultHttpClient();
//    try {
//      HttpResponse response = client.execute(uri);
//      if(response.getStatusLine().getStatusCode() != 200){
//        throw new RuntimeException(String.format("Call failed with status : %d",
//          response.getStatusLine().getStatusCode()));
//      }
//      ByteArrayOutputStream bos = new ByteArrayOutputStream();
//      ByteStreams.copy(response.getEntity().getContent(), bos);
//      return bos.toString("UTF-8");
//    } catch (IOException e) {
//      LOG.warn("Failed to retrieve data from " + uri.getURI().toASCIIString(), e);
//      return null;
//    } finally {
//      client.getConnectionManager().shutdown();
//    }
//  }
}

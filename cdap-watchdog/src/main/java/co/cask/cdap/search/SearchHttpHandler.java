/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.search;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.search.co.cask.cdap.search.run.LogSearchDocument;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * Handler that uses {@link Client} to perform operations on elastic search
 */
@Path(Constants.Gateway.API_VERSION_3 + "/search")
public class SearchHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SearchHttpHandler.class);
  private static final Gson GSON = new Gson();
  private final Client elasticClient;
  public SearchHttpHandler(Client client) {
    this.elasticClient = client;
  }

  @POST
  @Path("/indexes/log")
  public void search(HttpRequest request, HttpResponder responder) throws Exception {
    String body = request.getContent().toString(Charsets.UTF_8);
    LogSearchDocument document = GSON.fromJson(body, LogSearchDocument.class);

    XContentBuilder contentBuilder = jsonBuilder().startObject();
    contentBuilder.field("timestamp", document.getTimeStamp())
      .field("message", document.getMessage()).field("exception", document.isException());

    if (document.getExceptionClassName() != null) {
      contentBuilder.field("exceptionClassName", document.getExceptionClassName())
        .field("stacktrace", document.getStackTrace());
    }

    contentBuilder.field("logger", document.getLoggerName()).endObject();

    IndexResponse response = elasticClient.prepareIndex(document.getIndex(), document.getIndexType(),
                                                        String.valueOf(document.getTimeStamp()))
      .setSource(contentBuilder)
      .execute()
      .actionGet();

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/indexes/{index-name}/query")
  public void get(HttpRequest request, HttpResponder responder,
                  @PathParam("index-name") String indexName, @QueryParam("q") String queryString) throws Exception {
    SearchRequestBuilder searchBuilder = new SearchRequestBuilder(elasticClient);
    searchBuilder.setIndices(indexName);
    QueryBuilder query = QueryBuilders.queryStringQuery(queryString);
    searchBuilder.setQuery(query);
    SearchResponse response = searchBuilder.execute().actionGet();
    responder.sendString(HttpResponseStatus.OK, response.toString());
  }
}

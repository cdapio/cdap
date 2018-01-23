/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.explore.executor;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.service.ExploreTableManager;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handler that implements internal explore APIs.
 * Based off of ExploreExecutorHttpHandler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/data/explore")
public class Hive2HttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Hive2HttpHandler.class);

  private final ExploreTableManager exploreTableManager;
  private final DatasetFramework datasetFramework;
  private final StreamAdmin streamAdmin;
  private final SystemDatasetInstantiatorFactory datasetInstantiatorFactory;
  private final Impersonator impersonator;

  @Inject
  public Hive2HttpHandler(ExploreTableManager exploreTableManager,
                          DatasetFramework datasetFramework,
                          StreamAdmin streamAdmin,
                          SystemDatasetInstantiatorFactory datasetInstantiatorFactory,
                          Impersonator impersonator) {
    this.exploreTableManager = exploreTableManager;
    this.datasetFramework = datasetFramework;
    this.streamAdmin = streamAdmin;
    this.datasetInstantiatorFactory = datasetInstantiatorFactory;
    this.impersonator = impersonator;
  }

  @GET
  @Path("streams/testit/testit")
  public void testIt(HttpRequest request, HttpResponder responder)
    throws SQLException, IOException, ClassNotFoundException {
//    new HiveJdbcClient().main(new String[0]);
    responder.sendStatus(HttpResponseStatus.OK);
  }
}

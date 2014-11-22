/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.preferences;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.PreferenceTableDataset;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Preferences Handler.
 */
@Path(Constants.Gateway.GATEWAY_VERSION + "/preferences")
public class PreferencesHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreferencesHandler.class);
  private static final Type REQUEST_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new Gson();
  private final PreferenceTableDataset table;
  private final TransactionExecutor txExecutor;

  public PreferencesHandler(PreferenceTableDataset table, TransactionExecutorFactory executorFactory) {
    this.table = table;
    List<TransactionAware> txAwareList = Lists.newArrayList();
    txAwareList.add(this.table);
    this.txExecutor = executorFactory.createExecutor(txAwareList);
  }

  @Path("/programpreference/apps/{app-name}/{program-type}/{program-name}")
  @GET
  public void getPref(HttpRequest request, HttpResponder responder,
                      @PathParam("app-name") final String appId,
                      @PathParam("program-type") final String programType,
                      @PathParam("program-name") final String programName) throws Exception {
    final ProgramRecord record = new ProgramRecord(ProgramType.valueOf(programType.toUpperCase()), appId,
                                                   programName, null, null);
    final Map<String, String> notes = Maps.newHashMap();
    try {
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          notes.putAll(table.getNotes(record));
        }
      });
    } catch (TransactionFailureException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, "Transaction Conflict occurred. Retry request");
    }
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(notes));
  }

  @Path("/programpreference/apps/{app-name}/{program-type}/{program-name}")
  @PUT
  public void putPref(HttpRequest request, HttpResponder responder,
                      @PathParam("app-name") final String appId,
                      @PathParam("program-type") final String programType,
                      @PathParam("program-name") final String programName) throws Exception {
    final ProgramRecord record = new ProgramRecord(ProgramType.valueOf(programType.toUpperCase()), appId, programName,
                                                   null, null);
    final Map<String, String> notes = GSON.fromJson(new InputStreamReader(
      new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8), REQUEST_TYPE);
    try {
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          table.setNotes(record, notes);
        }
      });
    } catch (TransactionFailureException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, "Transaction Conflict occurred. Retry request");
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }
}

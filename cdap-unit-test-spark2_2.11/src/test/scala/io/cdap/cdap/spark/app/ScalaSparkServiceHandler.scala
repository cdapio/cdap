/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.spark.app

import io.cdap.cdap.api.service.http.HttpServiceRequest
import io.cdap.cdap.api.service.http.HttpServiceResponder
import io.cdap.cdap.api.spark.service.AbstractSparkHttpServiceHandler

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.QueryParam

import scala.collection.JavaConversions._

/**
  *
  */
class ScalaSparkServiceHandler extends AbstractSparkHttpServiceHandler {

  @GET
  @Path("/sum")
  def sum(request: HttpServiceRequest, responder: HttpServiceResponder,
          @QueryParam("n") numbers: java.util.List[Integer]) = {
    // Sum n numbers from the query param
    val sc = getContext.getSparkContext
    val result = sc.parallelize(numbers).reduce(_ + _)
    responder.sendString(result.toString)
  }
}

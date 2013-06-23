package com.continuuity.common.http.core;

import com.google.common.collect.Lists;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.List;

/**
 * HttpMethodHandler handles the http request. HttpMethodHandler looks up all Jax-rs annotations in classes
 * and dispatches to appropriate method on receiving requests.
 */
public class HttpMethodHandler {
  //TODO (ENG-2724): WIP. This needs to be implemented
  //Note: In the first cut the handlers needs to be passed in. Consider using guice to bind in later release.
  public HttpMethodHandler(List<Object> handlers){
    //Read the annotations from the class passed.
  }

  //TODO (ENG-2724): WIP. This needs to be implemented
  public void handle(HttpRequest request, HttpResponder responder){
    //Find out the appropriate Method and call
  }
}

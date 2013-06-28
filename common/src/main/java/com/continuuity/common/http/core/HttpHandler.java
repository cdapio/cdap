package com.continuuity.common.http.core;

/**
 *  Interface that needs to be implemented for handling HTTP methods.
 *  init and destroy methods can be used to manage the lifecycle of the object.
 *  The framework will call init and destroy during startup and shutdown respectively.
 *  The handlers should be annotated with Jax-RS annotations to handle appropriate path and HTTP Methods.
 *
 *  Example:
 *  public class ApiHandler implements HttpHandler{
 *    @Override
 *    public void init(){
 *      //Do some startup time stuff
 *    }
 *    @Override void destroy(){
 *     //Do some shutdown stuff
 *    }
 *
 *    @Path("/common/v1/widgets")
 *    @GET
 *    public void handleGet(HttpRequest request, HttpResponder responder){
 *      //Handle Http request
 *    }
 *  }
 *
 */
public interface HttpHandler {

  public void init();

  public void destroy();
}

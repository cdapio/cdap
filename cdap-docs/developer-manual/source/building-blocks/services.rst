.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _user-services:

========
Services
========

*Services* can be run in a Cask Data Application Platform (CDAP) application to serve data to external clients.
Similar to flows, services run in containers and the number of running service instances can be dynamically scaled.
Developers can implement custom services to interface with a legacy system and perform additional processing beyond
the CDAP processing paradigms. Examples could include running an IP-to-geo lookup and serving user-profiles.

The lifecycle of a custom service can be controlled via the CDAP UI, by using the
:ref:`CDAP Java Client API <client-api>`, or with the :ref:`CDAP RESTful HTTP API <restful-api>`.

You can add services to your application by calling the ``addService`` method in the
application's ``configure`` method::

  public class AnalyticsApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("AnalyticsApp");
      setDescription("Application for generating mobile analytics");
      addStream(new Stream("event"));
      addFlow(new EventProcessingFlow());
      ...
      addService(new IPGeoLookupService());
      addService(new UserLookupService());
      ...
    }
  }

Services are implemented by extending ``AbstractService``, which consists of
``HttpServiceHandler``\s to serve requests::

  public class IPGeoLookupService extends AbstractService {

    @Override
    protected void configure() {
      setName("IpGeoLookupService");
      setDescription("Service to lookup locations of IP addresses.");
      useDataset("IPGeoTable");
      addHandler(new IPGeoLookupHandler());
    }
  }

.. _user-service-handlers:

Service Handlers
================
``ServiceHandler``\s are used to handle and serve HTTP requests.

You add handlers to your service by calling the ``addHandler`` method in the service's
``configure`` method, as shown above. Only handler classes that are declared public,
with public methods for endpoints, will be exposed by the service.

To use a dataset within a handler, either include the ``@UseDataSet`` annotation in
the handler, or use the ``getDataset()`` method dynamically in the handler to obtain
an instance of the dataset (see :ref:`Using Datasets in Programs <datasets-in-programs>`).
Each request to a method is committed as a single transaction.

::

  public class IPGeoLookupHandler extends AbstractHttpServiceHandler {
    @UseDataSet("IPGeoTable")
    Table table;

    @Path("lookup/{ip}")
    @GET
    public void lookup(HttpServiceRequest request, HttpServiceResponder responder,
                                                      @PathParam("ip") String ip) {
      // ...
      responder.sendString(200, location, Charsets.UTF_8);
    }
  }


Path and Query Parameters
=========================
Handler endpoints can have Path and Query parameters. Path parameters are used to assist with path-mapping of requests,
while Query parameters are used to easily parse the query string of a request.

For example, the ``WordCount`` application has a ``Service`` that exposes an endpoint to retrieve the count of a word
and its word associations. In the ``@Path`` annotation, ``{word}`` is a path parameter that is mapped
to a Java String using ``@PathParam("word") String word``. Similarly, the endpoint also allows
the query parameter ``limit`` with a default value of 10.

::

  @Path("count/{word}")
  @GET
  public void getCount(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("word") String word,
                       @QueryParam("limit") @DefaultValue("10") Integer limit) {

    // ...
  }

An example of calling this endpoint with the HTTP RESTful API is shown in the :ref:`http-restful-api-service`.

**Note:** Any reserved or unsafe characters in the path parameters should be encoded using 
:ref:`percent-encoding <http-restful-api-conventions-reserved-unsafe-characters>`.
See the next section, :ref:`services-path-parameters`.

.. _services-content-consumer:

Handling a Large Request Body
=============================
Sometimes the request body for a ``PUT`` or ``POST`` request can be huge and it is not feasible to keep all
of it in memory. You can have the handler method return an ``HttpContentConsumer`` instead of ``void``
to process the request body in smaller pieces.

For example, the ``SportResults`` application has an ``UploadService`` that exposes an endpoint for uploading files
to ``PartitionedFileSets``. It returns an ``HttpContentConsumer`` so that it receives the request body in a series
of small chunks::

  @PUT
  @Path("leagues/{league}/seasons/{season}")
  public HttpContentConsumer write(HttpServiceRequest request, HttpServiceResponder responder,
                                   @PathParam("league") String league, @PathParam("season") int season) {
    // ...
  }

An example of how to implement ``HttpContentConsumer`` is shown in the :ref:`Sport Results Example <examples-sport-results>`.

.. _services-path-parameters:

About Path Parameters
=====================
The value of a path parameter cannot contain any `characters that have a special meaning
<http://tools.ietf.org/html/rfc3986#section-2.2>`__ in URI syntax. If a request has a path
parameter that contains such a character, it must be `URL-encoded
<http://tools.ietf.org/html/rfc3986#section-2.1>`__ using the "``%hh``" notation, a
percent-symbol followed by two hex characters. 

In general, any character that is not a letter, a digit, or one of ``$-_.+!*'()`` should be encoded.

However, if the special character is a forward-slash (``/``), then it will appear to the
path matcher as a "``/``", even if it is escaped as "``%2f``". This occurs because the path is
decoded prior to matching.

There are two ways to work around this:

- Double-escape any forward-slashes (``/``) as "``%252f``". This will prevent the decoding before the path is matched.
  However, the path parameter's value will contain the "``%2f``" instead of a "``/``", and the
  application code must decode the parameter itself to obtain the actual value.

- Use a query parameter instead. This is a better solution because the "``/``" is not a reserved
  character in the query of a URI.

Service Discovery
=================
Services announce the host and port they are running on so that they can be discovered |---| and
accessed |---| by other programs.

Service are announced using the name passed in the ``configure`` method. The *application name*, *service id*, and
*hostname* required for registering the service are automatically obtained.

The service can then be discovered in a flow, MapReduce, Spark, or another service using
the appropriate program context. You may also access a service in a different application
by specifying the application name in the ``getServiceURL`` call.

For example, in flows::

  public class GeoFlowlet extends AbstractFlowlet {

    // URL for IPGeoLookupService
    private URL serviceURL;

    // URL for SecurityService in SecurityApplication
    private URL securityURL;

    @ProcessInput
    public void process(String ip) {
      // Get URL for service in same application
      serviceURL = getContext().getServiceURL("IPGeoLookupService");

      // Get URL for service in a different application
      securityURL = getContext().getServiceURL("SecurityApplication", "SecurityService");

      // Access the IPGeoLookupService using its URL
      if (serviceURL != null) {
        URLConnection connection = new URL(serviceURL, String.format("lookup/%s", ip)).openConnection();
        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      }
      ...
      // Access the SecurityService using its URL
      if (securityURL != null) {
        ...
      }
    }
  }

Services and Resources
======================
When a service is configured, the resource requirements for the server that runs all
handlers of the service can be set, both in terms of the amount of memory (in megabytes)
and the number of virtual cores assigned.

For example, in the :ref:`Purchase <examples-purchase>` example, in the configuration of
the ``PurchaseHistoryService``, the amount of memory is specified:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryService.java
   :language: java
   :lines: 40-46

If both the memory and the number of cores needs to be set, this can be done using::

    setResources(new Resources(1024, 2));

The resource requirements can also be altered through runtime arguments,
as explained in :ref:`Configuring Resources <advanced-configuring-resources>`.

An example of setting ``Resources`` using runtime arguments is shown in :ref:`Purchase
<examples-purchase>` example's ``PurchaseHistoryBuilder.java``.

.. _services-routing:

Service Thread Model
====================
An HTTP server is started for each Service instance, which by default starts 60 threads to handle
client requests. Each thread is basically tied to one active client request and each thread would
have its own instance of ``HttpServiceHandler``\s. This guarantees there will be no concurrent
calls to each ``HttpServiceHandler`` object instance. Also, by default, when a thread is idled
for more than 60 seconds, it will be terminated automatically, with the ``HttpServiceHandler.destroy``
method being called to release resources.

Both the number of service threads and the thread keep-alive time can be altered by these runtime arguments:

- ``system.service.threads``: Number of threads to use in the HTTP server
- ``system.service.thread.keepalive.secs``: Number of seconds a thread can sit idle before getting terminated


Service Routing
===============
When multiple versions of the same service are running, you can control where service
requests are routed.

For example, if version ``v1`` and version ``v2`` of the same application are running, you
can choose to direct 50% of the requests to version ``v1`` of the service and 50% to version
``v2``. This can be achieved by uploading a *route configuration* (also known as a *route
config*): a map of version names to the percentage of requests to be routed to that
version.

For a specific service, if a route config is not present or if it cannot be retrieved, a
fallback routing strategy is used. The strategy used can be configured in the
``cdap-site.xml`` file.

These fallback strategies are available: *random*, *smallest*, *largest*, and *drop*.

**Random** is the default fallback strategy. If the random fallback strategy is chosen,
the request is routed to any version of the service.

If **smallest** is chosen, the request is routed to the smallest version (based on a
string comparison of the available versions). Similarly for **largest**: the request
is routed to the largest version. If **drop** is chosen as the fallback strategy, the
request is not routed to any version.

The fallback strategy can be configured using the property
``router.userservice.fallback.strategy`` in the 
:ref:`cdap-site.xml file <appendix-cdap-site.xml>`.

For information on how to store, fetch, and delete a routing configuration
(*RouteConfig*), refer to the :ref:`Route Config HTTP RESTful API documentation 
<http-restful-api-route-config>`.

Services Examples
=================
- The simplest example, :ref:`Hello World <examples-hello-world>`, demonstrates using a
  service to **retrieve a name from a dataset.**
  
- The :ref:`Purchase example <examples-purchase>` includes two services, ``CatalogLookupService``
  and ``PurchaseHistoryService``; the latter **retrieves a specified customer's purchase 
  history in a JSON format from a dataset.**

- The :ref:`Spark example <examples-spark-k-means>` includes a service that **responds with
  a calculated center from a dataset based on an index parameter.**

- For another example of **a service reading from a dataset,** see the :ref:`Spark PageRank
  example <examples-spark-page-rank>`.

- For an example of **using path and query parameters,** see the 
  :ref:`WordCount example <examples-word-count>`, where the class ``RetrieveCountsHandler``
  retrieves a variety of statistics from datasets depending on the path supplied. 

- Almost all of the :ref:`how-to guides <guides-index>` demonstrate the use of services.
  (The exception is the :ref:`cdap-bi-guide`.)

- From the :ref:`tutorials`, the *WISE: Web Analytics* and the 
  *MovieRecommender: Recommender System* both demonstrate the use of services.

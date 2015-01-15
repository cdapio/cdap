.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _user-services:

========
Services
========

Services can be run in a Cask Data Application Platform (CDAP) Application to serve data to external clients.
Similar to Flows, Services run in containers and the number of running service instances can be dynamically scaled.
Developers can implement Custom Services to interface with a legacy system and perform additional processing beyond
the CDAP processing paradigms. Examples could include running an IP-to-Geo lookup and serving user-profiles.

The lifecycle of a Custom Service can be controlled via the CDAP Console, by using the
:ref:`CDAP Java Client API <client-api>`, or with the :ref:`CDAP RESTful HTTP API <restful-api>`.

You can add Services to your application by calling the ``addService`` method in the
Application's ``configure`` method::

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

.. Similarly, you can also add Services using the ``addLocalService`` method. These Services
.. will only be accessible by other programs within the same Application—other Applications
.. and external clients will not be able to use them.

Service Handlers
----------------

``ServiceHandler``\s are used to handle and serve HTTP requests.

You add handlers to your Service by calling the ``addHandler`` method in the Service's
``configure`` method, as shown above.

To use a Dataset within a handler, specify the Dataset by calling the ``useDataset``
method in the Service's ``configure`` method and include the ``@UseDataSet`` annotation in
the handler to obtain an instance of the Dataset. Each request to a method is committed as
a single transaction.

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


.. _services-path-parameters:

About Path Parameters
---------------------
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
-----------------

Services announce the host and port they are running on so that they can be discovered—and
accessed—by other programs.

Service are announced using the name passed in the ``configure`` method. The *application name*, *service id*, and
*hostname* required for registering the Service are automatically obtained.

The Service can then be discovered in Flows, Procedures, MapReduce programs, Spark programs, and other Services using
appropriate program contexts. You may also access Services in a different Application
by specifying the Application name in the ``getServiceURL`` call.

For example, in Flows::

  public class GeoFlowlet extends AbstractFlowlet {

    // URL for IPGeoLookupService
    private URL serviceURL;

    // URL for SecurityService in SecurityApplication
    private URL securityURL;

    @ProcessInput
    public void process(String ip) {
      // Get URL for Service in same Application
      serviceURL = getContext().getServiceURL("IPGeoLookupService");

      // Get URL for Service in a different Application
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

.. rubric::  Examples of Using Services

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


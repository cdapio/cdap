.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _user-services:

============================================
Services
============================================

Services can be run in a Cask Data Application Platform (CDAP) Application to serve data to external clients.
Similar to Flows, Services run in containers and the number of running service instances can be dynamically scaled.
Developers can implement Custom Services to interface with a legacy system and perform additional processing beyond
the CDAP processing paradigms. Examples could include running an IP-to-Geo lookup and serving user-profiles.

The lifecycle of a Custom Service can be controlled via the CDAP Console or by using the
:ref:`CDAP Java Client API <client-api>` or :ref:`CDAP RESTful HTTP API <restful-api>`.

Services are implemented by extending ``AbstractService``, which consists of ``HttpServiceHandler``\s to serve requests.

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

::

  public class IPGeoLookupService extends AbstractService {

    @Override
    protected void configure() {
      setName("IpGeoLookupService");
      setDescription("Service to lookup locations of IP addresses.");
      useDataset("IPGeoTable");
      addHandler(new IPGeoLookupHandler());
    }
  }

Service Handlers
----------------

``ServiceHandler``\s are used to handle and serve HTTP requests.

You add handlers to your Service by calling the ``addHandler`` method in the Service's ``configure`` method.

To use a Dataset within a handler, specify the Dataset by calling the ``useDataset`` method in the Service's
``configure`` method and include the ``@UseDataSet`` annotation in the handler to obtain an instance of the Dataset.
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

Service Discovery
-----------------

Services announce the host and port they are running on so that they can be discovered—and
accessed—by other programs.

Service are announced using the name passed in the ``configure`` method. The *application name*, *service id*, and
*hostname* required for registering the Service are automatically obtained.

The Service can then be discovered in Flows, Procedures, MapReduce jobs, and other Services using
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
      URLConnection connection = new URL(serviceURL, String.format("lookup/%s", ip)).openConnection();
      BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      ...
    }
  }

.. rubric::  Examples of Using Services

- Almost all of the :ref:`how-to guides <guides-index>` demonstrate the use of services.
  (The exception is the :ref:`cdap-bi-guide`.)

- From the :ref:`tutorials`, the *WISE: Web Analytics* and the 
  *MovieRecommender: Recommender System* both demonstrate the use of services.


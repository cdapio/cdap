.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _operations-metrics:

============================================
Metrics
============================================

.. highlight:: java

As applications process data, the CDAP collects metrics about the application’s behavior
and performance. Some of these metrics are the same for every application—how many events
are processed, how many data operations are performed—and are thus called system or CDAP
metrics.

Other metrics are user-defined or "custom" and differ from application to application.
To add user-defined metrics to your application, read this section in conjunction with the
details on available system metrics in the :ref:`Metrics HTTP API. <http-restful-api-metrics>`

You embed user-defined metrics in the methods defining the elements of your application.
With this Metrics instance , you can can emit metrics identified by ``metricName`` and an associated value for the ``metricName``.
The Metrics system currently supports two kinds of metrics:
  - count(metricName, delta)
    Increments/Decrements the ``metricsName`` value by delta
  - gauge(metricName, value)
    Sets the ``metricName`` to the given value

They will then emit their metrics and you can retrieve them (along with system metrics)
via the *Metrics Explorer* in the :ref:`CDAP Console <cdap-console>` or
via the CDAP’s :ref:`restful-api`. The names given to the metrics (such as
``names.longnames`` and ``names.bytes`` as in the example below) should be composed only
of alphanumeric characters.

To add a count metric to a Flowlet *NameSaver*::

  public static class NameSaver extends AbstractFlowlet {
    static final byte[] NAME = { 'n', 'a', 'm', 'e' };

    @UseDataSet("whom")
    KeyValueTable whom;
    Metrics flowletMetrics; // Declare the custom metrics

    @ProcessInput
    public void processInput(StreamEvent event) {
      byte[] name = Bytes.toBytes(event.getBody());
      if (name != null && name.length > 0) {
        whom.write(NAME, name);
      }
      if (name.length > 10) {
        flowletMetrics.count("names.longnames", 1);
      }
      flowletMetrics.count("names.bytes", name.length);
    }
  }

To add a gauge metric to the Flowlet *PurchaseStreamReader*::

  public class PurchaseStreamReader extends AbstractFlowlet {
      private OutputEmitter<Purchase> out;
      private Metrics cacheSize;
      private Cache<String, Integer> spendingCustomers = CacheBuilder.newBuilder().build();

      @ProcessInput
      public void process(StreamEvent event) {
        String body = Bytes.toString(event.getBody());
        // <name> bought <n> <items> for $<price>
        String[] tokens =  body.split(" ");
        if (tokens.length != 6) {
          return;
        }
        String customer = tokens[0];
        if (!"bought".equals(tokens[1])) {
          return;
        }
        int quantity = Integer.parseInt(tokens[2]);
        String item = tokens[3];
        if (quantity != 1 && item.length() > 1 && item.endsWith("s")) {
          item = item.substring(0, item.length() - 1);
        }
        if (!"for".equals(tokens[4])) {
          return;
        }
        String price = tokens[5];
        if (!price.startsWith("$")) {
          return;
        }
        int amount = Integer.parseInt(tokens[5].substring(1));

        if (amount > 1000) {
          Integer items = spendingCustomers.getIfPresent(customer);
          items = (items == null) ? quantity : items + quantity;
          spendingCustomers.put(customer, items);
        }

        Purchase purchase = new Purchase(customer, item, quantity, amount, System.currentTimeMillis());
        out.emit(purchase);
      }

      @Tick(delay = 5L, unit = TimeUnit.SECONDS)
      public void generate() throws Exception {
        // emit cache size every 5 seconds as the metric and flush out the cache
        cacheSize.gauge("top.spending.customers", spendingCustomers.size());
        spendingCustomers.invalidateAll();
      }
    }

An example of user-defined metrics is in ``PurchaseStore`` in the :ref:`Purchase example. <examples-purchase>`

Using Metrics Explorer
----------------------
The *Metrics Explorer* of the :ref:`CDAP Console <cdap-console>`
can be used to examine and set metrics in a CDAP instance.

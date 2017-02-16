.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: Transactional Messaging System, a ACID-guaranteed "publish-and-subscribe" messaging service

.. _transactional-messaging-system:

==============================
Transactional Messaging System
==============================

.. wiki: https://wiki.cask.co/display/CE/Messaging

Overview
========
The Transactional Messaging System (TMS) is a CDAP service that provides a
"publish-and-subscribe" messaging system that understands transactions.

It uses HBase for the persistent and durable storage of messages, and supports both
transactional and non-transactional message publishing and consumption.

TMS provides ACID *(atomicity, consistency, isolation,* and *durability)* guarantees,
using `Apache Tephra <http://tephra.incubator.apache.org>`__ for the ACI portion. TMS
doesn't interact directly with Tephra; instead, the client provides the transaction, as
described below.

TMS is similar to `Apache Kafka <https://kafka.apache.org>`__ but with added guarantees as
to ordering and persistence of messages.

**Note:** TMS is currently a **beta** feature of CDAP |release|, and is subject to change
without notice.


Topics
======
Topics are created in TMS using both a namespace and a topic name. Note that while it is
possible to fetch from ``system`` namespace, attempting to publish to the ``system``
namespace will result in an error.

Currently, as TMS :ref:`does not use authorization <tms-limitations>`, you can only create
topics in the same namespace as your application is running in. However, you can publish
to all topics in all namespaces (except the `system` namespace, as noted above) and fetch
from all topics in all namespaces.


Message Publishing
==================
Messages in TMS are published as either `non-transactional <Non-transactional Messages>`__
or `transactional <Transactional Messages>`__ messages.

Non-transactional Message Publishing
------------------------------------
Non-transactional messaging in TMS works in a manner very similar to `Apache Kafka
<https://kafka.apache.org>`__: 

- **publish** a message (also called a *payload*) to a topic
- **fetch** (also called *subscribe* or *consume*) from a topic

TMS provides strong ordering guarantees for the consumers of a topic:

- When fetching from the same topic, every consumer will see the exact same order of
  messages.

- A consumer, fetching from the same topic more than once from the same point, will always
  see the topics in the exact same order.

Each message has a timestamp, which can be thought of as the published time, or the time
the system persisted the message. Each message ID, combined with a topic, uniquely
identifies each message.

Under a high-concurrent load, the actual ordering of messages will be arbitrary, but is
guaranteed to be consistent when fetched. With non-transactional messages, messages are
available for consumption (fetching) as soon as the method call that publishes them returns.

Note that non-transactional consumers see *all* messages of a topic, including messages that
are currently in a transaction.

Transactional Message Publishing
--------------------------------
With transactional messages, messages are not available (published) until the transaction
has been successfully committed.

For example, a pipeline or flow might follow these steps:

- Open a transaction
- Do some work
- Publish a message
- Do additional work
- Commit the transaction
- If the transaction is successful: message is now visible to *transactional* consumers
- If the transaction is unsuccessful: message is rolled back and is never seen by
  *transactional* consumers

However, as noted above, *non-transactional* consumers see all messages of a topic,
including messages that are currently in a transaction.

With transactional publishing, all the work in a transaction will appear atomically to
downstream consumers who are also transactional. It is not necessary that those consumers
be in the *same* transaction; instead, they merely need to be in a transaction themselves.

Example Publish and Subscribe
=============================
Consider a flow that modifies a database, and at the same time publishes
a notification to a topic.

If it were to publish to a Kafka topic, a problem can arise as there is no guarantee that
the Kafka notification will be published only after the database commit:

**Kafka Topic Example**

::

  Flow 1

  [ flowlet 1.1 ] -> [ flowlet 1.2 ] -> Writes to a Kafka topic
                                     -> Writes to a dataset
                               
  Flow 2

  [ flowlet 2.1 ] -> Watches Kafka topic for messages
                  - May see the message about the write before the write completes or even if the write was rolled back 

However, with publishing to a TMS topic transactionally, there is the guarantee that
transaction consumers will only see the notification if the write to the database is
successfully committed:

**TMS Topic Example**

::

  Flow 1 (explicit transaction)
                                                               Transaction A
  [ flowlet 1.1 ] -> [[ flowlet 1.2 ] -> Writes to a dataset   ]
                     [                -> Writes to a TMS topic ] This is now only visible if the write to the dataset succeeds 
                               
  Flow 2 (explicit transaction)

                  transaction B
  [[ flowlet 2.1 ]] -> Watches TMS topic for messages
                    - Only sees the message if the write was successful
                    - Guaranteed to see messages in the correct order of publishing 


Currently, TMS:

- Only supports explicit transactions 
- Does not support publishing from a long-running transaction, such as a mapper, reducer, or Spark executor.


Code Examples
=================

These examples all run in a `worker <workers>`. For fetching messages, they use a common
method to fetch and block until either a message is received or a timeout is reached.

.. rubric:: Utility method for blocking and fetching a message

::

  /**
   * Fetch and block until it get a message.
   */
  private static Message fetchMessage(MessageFetcher fetcher, String namespace, String topic,
                                      @Nullable String afterMessageId, long timeout, TimeUnit unit) throws Exception {
    CloseableIterator<Message> iterator = fetcher.fetch(namespace, topic, 1, afterMessageId);
    Stopwatch stopwatch = new Stopwatch().start();
    try {
      while (!iterator.hasNext() && stopwatch.elapsedTime(unit) < timeout) {
        TimeUnit.MILLISECONDS.sleep(100);
        iterator = fetcher.fetch(namespace, topic, 1, afterMessageId);
      }

      if (!iterator.hasNext()) {
        throw new TimeoutException("Failed to get any messages from " + topic +
                                     " in " + timeout + " " + unit.name().toLowerCase());
      }
      // The payload contains the message to publish in next step
      return iterator.next();
    } finally {
      iterator.close();
    }
  }

.. rubric:: Creating a topic

::

  public static final class MessagingWorker extends AbstractWorker {
  
    static final String TOPIC = "topic"
  
    @Override
    public void run() {
      try {
        // Create a topic
        getContext().getAdmin().createTopic(TOPIC);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }  

..  rubric:: Publishing a message to a topic non-transactionally

::

  public static final class MessagingWorker extends AbstractWorker {
  
    static final String TOPIC = "topic"
  
    @Override
    public void run() {
      try {
        final MessagePublisher publisher = getContext().getMessagePublisher();
        String payload = "Message to send";
        publisher.publish(getContext().getNamespace(), TOPIC, payload);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }  

..  rubric:: Publishing a message to a topic transactionally

::

  public static final class MessagingWorker extends AbstractWorker {
  
    static final String TOPIC = "topic"
  
    @Override
    public void run() {
      try {
        final MessagePublisher publisher = getContext().getMessagePublisher();
        String payload = "Message to send";
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            // Publish the message transactionally.
            publisher.publish(getContext().getNamespace(), TOPIC, payload);
          }
        });
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }  

..  rubric:: Fetching from a topic non-transactionally

::

  public static final class MessagingWorker extends AbstractWorker {
  
    static final String TOPIC = "topic"
  
    @Override
    public void run() {
      try {
        final MessageFetcher fetcher = getContext().getMessageFetcher();
        // Block until either a message is received or the timeout is reached
        Message message = fetchMessage(fetcher, getContext().getNamespace(), TOPIC, null, 10, TimeUnit.SECONDS);
        String payload = message.getPayloadAsString();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }  

..  rubric:: Fetching from a topic transactionally

::

  public static final class MessagingWorker extends AbstractWorker {
  
    static final String TOPIC = "topic"
  
    @Override
    public void run() {
      try {
        final MessageFetcher fetcher = getContext().getMessageFetcher();
        
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            // Block until either a message is received or the timeout is reached
            Message message = fetchMessage(fetcher, getContext().getNamespace(), TOPIC, null, 10, TimeUnit.SECONDS);
            String payload = message.getPayloadAsString();
          }
        });
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }  


Java API
========
Javadocs describing the TMS Java API are available in the 
:javadoc:`package co.cask.cdap.api.messaging <co/cask/cdap/api/messaging/package-summary>`:

- :javadoc:`MessagingAdmin <co/cask/cdap/api/messaging/MessagingAdmin>`: Provides topic administration functions
- :javadoc:`MessagingContext <co/cask/cdap/api/messaging/MessagingContext>`: Provides access to the Transactional Messaging System
- :javadoc:`MessagePublisher <co/cask/cdap/api/messaging/MessagePublisher>`: Provides message publishing functions
- :javadoc:`MessageFetcher <co/cask/cdap/api/messaging/MessageFetcher>`: Provides message fetching functions

.. _tms-limitations:

Limitations
===========
Currently, TMS does not use authorization, and does not allow creating topics outside of the current namespace.

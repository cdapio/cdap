.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: Transactional Messaging System, a ACID-guaranteed "publish-and-subscribe" messaging service

.. _transactional-messaging-system:

==============================
Transactional Messaging System
==============================

.. topic::  **Note:** 

    TMS is currently a **beta** feature of CDAP |release|, and is subject to change without notice.


Overview
========
The Transactional Messaging System (TMS) is a CDAP service that provides a
"publish-and-subscribe" messaging system that understands transactions, and that
guarantees the ordering and persistence of messages.

It uses HBase for the persistent and durable storage of messages, and supports both
transactional and non-transactional message publishing and consumption.


Topics
======
Topics are created in TMS using both a namespace and a topic name. Note that while it is
possible to fetch from ``system`` namespace, attempting to publish to the ``system``
namespace will result in an error.

Currently, as TMS :ref:`does not use authorization <tms-limitations>`, you can only create
topics in the same namespace as your application is running in. For that reason, you do
not require the name of the current namespace to create a topic. However, you can publish
to all topics in all namespaces (except the `system` namespace, as noted above) and fetch
from all topics in all namespaces.

A valid topic name can only contain characters from the
:ref:`supported-characters-alphanumeric-extra-extended-character-set`, as described in the
:ref:`supported-characters`: alphanumeric characters, underscores (``_``), or hyphens
(``-``).


Messages
========
Messages in TMS are published as either `non-transactional <Non-transactional Messages>`__
or `transactional <Transactional Messages>`__ messages.

Non-transactional Messaging
---------------------------
Non-transactional messaging in TMS works in a manner very similar to other messaging systems:

- **publish** a message (also called a *payload*) to a topic
- **fetch** (also called *subscribe* or *consume*) from a topic

TMS provides strong ordering guarantees for the consumers of a topic:

- When fetching from the same topic, every consumer will see the exact same order of
  messages.

- A consumer, fetching from the same topic more than once from the same point, will always
  see the topics in the exact same order.

Each message has a timestamp, which can be thought of as the published time, or the time
the system persisted the message. Messages are uniquely identified by a concatenation of
the timestamp, the topic, and a sequence ID (for distinguishing messages published
in the same millisecond).

Under a high-concurrent load, the actual ordering of messages will be arbitrary, but is
guaranteed to be consistent when fetched. With non-transactional messages, messages are
available for consumption (fetching) as soon as the method call that publishes them returns.

Note that non-transactional consumers see *all* messages of a topic, including messages that
are currently in a transaction.

Transactional Messaging
-----------------------
With transactional messages, messages are not available (published) until the transaction
has been successfully committed.

For example, a pipeline or flow might follow these steps:

- Open a transaction
- Do some work
- Publish a message
- Do additional work
- Commit the transaction
- If the transaction is successful: the message is now visible to *transactional* consumers
- If the transaction is unsuccessful: the message is rolled back and is never seen by
  *transactional* consumers

However, as noted above, *non-transactional* consumers see all messages of a topic,
including the messages that were published and are currently in a transaction.

With transactional publishing, all the work in a transaction will appear atomically to
downstream consumers who are also transactional. It is not necessary that those consumers
be in the *same* transaction; instead, they merely need to be in a transaction themselves.


Example Publish and Subscribe
=============================
Consider a workflow that modifies a dataset, and at the same time publishes a notification to
a topic.

If it were to **publish to a topic non-transactionally,** a problem can arise as there is
no guarantee that the notification will be published only after the dataset commit. 
If it were to **publish transactionally to a TMS topic,** there is the guarantee that
transaction consumers will only see the notification if the write to the dataset is
successfully committed:

.. figure:: /_images/tms-diagram.png
  :figwidth: 100%
  :width: 600px
  :align: center

  **Transactional Example**


Currently, TMS:

- only supports explicit transactions; and
- does not support publishing from a long-running transaction, such as a mapper, reducer, or Spark executor.


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

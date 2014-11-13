.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Logging
============================================

.. highlight:: java

CDAP supports logging through standard
`SLF4J (Simple Logging Facade for Java) <http://www.slf4j.org/manual.html>`__ APIs.
For instance, in a Flowlet you can write::

  private static Logger LOG = LoggerFactory.getLogger(WordCounter.class);
  ...
  @ProcessInput
  public void process(String line) {
    LOG.info("{}: Received line {}", this.getContext().getTransactionAwareName(), line);
    ... // processing
    LOG.info("{}: Emitting count {}", this.getContext().getTransactionAwareName(), wordCount);
    output.emit(wordCount);
  }

The log messages emitted by your Application code can be viewed in two different ways.

- Using the :ref:`restful-api`.
  The :ref:`Logging HTTP interface <http-restful-api-logging>` details all the available contexts that
  can be called to retrieve different messages.
- All log messages of an Application can be viewed in the CDAP Console
  by clicking the *Logs* button in the Flow or Procedure screens.
  This launches the *Log Explorer*.

See the *Flow Log Explorer* of the :ref:`CDAP Console <cdap-console>` for details of using
it to examine logs in the CDAP. In a similar fashion, *Procedure Logs* can be examined
from within the CDAP Console.

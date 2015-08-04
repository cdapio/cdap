.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============
Stream Event
============

A stream event is a special type of object that comes in via streams. It
consists of a set of headers represented by a map from String to String,
and a byte array as the body of the event. To consume a stream with a
flow, define a flowlet that processes data of type ``StreamEvent``::

  class StreamReader extends AbstractFlowlet {
    ...
    @ProcessInput
    public void processEvent(StreamEvent event) {
      ...
    }

.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Stream Event
============================================

A Stream event is a special type of object that comes in via Streams. It
consists of a set of headers represented by a map from String to String,
and a byte array as the body of the event. To consume a Stream with a
Flow, define a Flowlet that processes data of type ``StreamEvent``::

  class StreamReader extends AbstractFlowlet {
    ...
    @ProcessInput
    public void processEvent(StreamEvent event) {
      ...
    }

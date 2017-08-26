# coding=utf-8
#
# Copyright © 2017 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import os
from threading import RLock

from py4j.java_gateway import java_import, JavaGateway

try:
  # The JavaObject is only needed for the Spark 1 hack. Failure to import in future Spark/py4j version is ok.
  from py4j.java_gateway import JavaObject
except:
  pass
try:
  # The CallbackServer is only needed for the Spark < 1.6 hack. Failure to import in future Spark/py4j version is ok.
  from py4j.java_gateway import CallbackServer
except:
  pass

__all__ = ["SparkExecutionContext", "Metrics", "ServiceDiscoverer"]

class SparkExecutionContext(object):
  """
    Spark program execution context. User Spark program can interact with CDAP through this context.
  """

  def __init__(self):
    self._runtimeContext = SparkRuntimeContext()

  def getLogicalStartTime(self):
    """
      Returns the logical start time of this Spark job. Logical start time is the time when this Spark
      job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
      job runs.

      :return:
        Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
    """
    return self._runtimeContext.getSparkRuntimeContext().getLogicalStartTime()


  def getRuntimeArguments(self):
    """
      Gets the set of arguments provided to this execution.
      :return:
        A dictionary of argument key and value
    """
    return self._runtimeContext.getSparkRuntimeContext().getRuntimeArguments()

  def getMetrics(self):
    """
      Returns a :class:`Metrics` object which can be used to emit custom metrics from the Spark program.
      This can also be passed in closures and workers can emit their own metrics.

      :return:
        a :class:`Metrics` object
    """
    return Metrics(self._runtimeContext)

  def getServiceDiscoverer(self):
    """
      Returns a :class:`ServiceDiscoverer` for Service Discovery
      in Spark Program which can be passed in closures.

      :return:
        a :class:`ServiceDiscoverer` object
    """
    return ServiceDiscoverer(self._runtimeContext)


class Metrics(object):
  """
    This class is for user program to emit metrics to CDAP.
  """

  def __init__(self, runtimeContext = None):
    self._runtimeContext = runtimeContext
    self._metrics = runtimeContext.getSparkRuntimeContext()

  def __getstate__(self):
    return { "context" : self._runtimeContext }

  def __setstate__(self, state):
    self.__init__(state["context"])

  def count(self, name, delta):
    """
      Increases the value of the specific metric by delta.

      :param name: Name of the counter. Use alphanumeric characters in metric names
      :param delta: The value to increase by
    """
    self._metrics.count(name, delta)

  def gauge(self, name, value):
    """
      Sets the specific metric to the provided value

      :param name: Name of the counter. Use alphanumeric characters in metric names
      :param value: The value to be set
    """
    self._metrics.gauge(name, value)

class ServiceDiscoverer(object):
  """
    This class provides discovery service to user program.
  """

  def __init__(self, runtimeContext = None):
    self._runtimeContext = runtimeContext
    self._discover = runtimeContext.getSparkRuntimeContext()

  def __getstate__(self):
    return { "context" : self._runtimeContext }

  def __setstate__(self, state):
    self.__init__(state["context"])

  def getServiceURL(self, serviceId, appId = None):
    """
      Discover the base URL for a Service, relative to which Service endpoints can be accessed.

      :param serviceId: name of the service to be discovered
      :param appId: an optional application name that the service belongs to
      :return: an URL :class:`str` for the discovered service or `None` if the service is not found
    """
    if appId is None:
      return self.getServiceURL(serviceId)

    url = self._discover.getServiceURL(appId, serviceId)
    if url is None:
      return None
    else:
      return str(url.toString())

class SparkRuntimeContext(object):
  """
    Private class that mirrors functionality for the Java counterpart.
  """

  _lock = RLock()
  _gateway = None
  _jvm = None
  _runtimeContext = None
  _onDemandCallback = False

  def __init__(self, gatewayPort = None, driver = True):
    # If the gateway port file is there, always use it. This is for distributed mode.
    if os.path.isfile("cdap.py4j.gateway.port.txt"):
      fd = open("cdap.py4j.gateway.port.txt", "r")
      gatewayPort = int(fd.read())
      fd.close()
    elif gatewayPort is None:
      if "PYSPARK_GATEWAY_PORT" in os.environ:
        gatewayPort = int(os.environ["PYSPARK_GATEWAY_PORT"])
      else:
        raise Exception("Cannot determine Py4j GatewayServer port")

    self.__class__.__ensureGatewayInit(gatewayPort, driver)
    self._allowCallback = driver
    self._gatewayPort = gatewayPort

  def __getstate__(self):
    return { "gatewayPort" : self._gatewayPort }

  def __setstate__(self, state):
    self.__init__(state["gatewayPort"], False)

  def getSparkRuntimeContext(self):
    return self.__class__._runtimeContext

  @classmethod
  def __ensureGatewayInit(cls, gatewayPort, driver):
    with cls._lock:
      if not cls._gateway:
        # Spark 1.6 and Spark 2 are using later verions of py4j (0.9 and 0.10+ respectively),
        # which has better control on gateway client and callback server using
        # GatewayParameters and CallbackServerParameters. Try to use those,
        # as it'll be less hacky (it's still a bit hacky for Spark 1.6, see below)
        try:
          from py4j.java_gateway import GatewayParameters, CallbackServerParameters
          callbackServerParams = CallbackServerParameters(port = 0, daemonize = True,
                                                          daemonize_connections = True) if driver else None
          gateway = JavaGateway(gateway_parameters = GatewayParameters(port = gatewayPort, auto_convert = True),
                                callback_server_parameters = callbackServerParams)
        except:
          from py4j.java_gateway import CallbackServer, GatewayClient
          gateway = JavaGateway(gateway_client = GatewayClient(port = gatewayPort), auto_convert = True)
          cls._onDemandCallback = True

        java_import(gateway.jvm, "co.cask.cdap.app.runtime.spark.*")
        java_import(gateway.jvm, "co.cask.cdap.app.runtime.spark.python.*")

        if driver and not cls._onDemandCallback:
          # For py4j 0.10+ (used by Spark 2.0), use the official API to set set callback port on the gateway server
          if "get_callback_server" in dir(gateway):
            callbackPort = gateway.get_callback_server().get_listening_port()
            gateway.jvm.SparkPythonUtil.setGatewayCallbackPort(gateway.java_gateway_server, callbackPort)
          else:
            # For py4j 0.9 (used by Spark 1.6), it doesn't have way to set the dynamic port of the callback server,
            # hence we need the hack to call SparkPythonUtil to set it
            callbackPort = gateway._callback_server.server_socket.getsockname()[1]
            gateway.jvm.SparkPythonUtil.setGatewayCallbackPort(JavaObject("GATEWAY_SERVER", gateway._gateway_client),
                                                               callbackPort)
        cls._gateway = gateway
        cls._jvm = gateway.jvm
        cls._runtimeContext = cls._jvm.SparkRuntimeContextProvider.get()
        print "Java gateway initialized with gateway port ", gatewayPort


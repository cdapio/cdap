/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.gateway.router

import co.cask.cdap.common.conf.Constants
import co.cask.cdap.common.discovery.ResolvingDiscoverable
import co.cask.http.AbstractHttpHandler
import co.cask.http.ChunkResponder
import co.cask.http.HttpResponder
import co.cask.http.NettyHttpService
import com.google.common.base.Charsets
import com.google.common.base.Function
import com.google.common.base.Splitter
import com.google.common.base.Supplier
import com.google.common.collect.ImmutableMultimap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import com.google.common.util.concurrent.AbstractIdleService
import com.ning.http.client.AsyncCompletionHandler
import com.ning.http.client.AsyncHttpClient
import com.ning.http.client.AsyncHttpClientConfig
import com.ning.http.client.HttpResponseBodyPart
import com.ning.http.client.Request
import com.ning.http.client.RequestBuilder
import com.ning.http.client.Response
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider
import org.apache.http.Header
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.twill.common.Cancellable
import org.apache.twill.discovery.Discoverable
import org.apache.twill.discovery.DiscoveryService
import org.apache.twill.discovery.DiscoveryServiceClient
import org.apache.twill.discovery.InMemoryDiscoveryService
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.SimpleChannelHandler
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.PrintWriter
import java.net.HttpURLConnection
import java.net.Socket
import java.net.URI
import java.net.URISyntaxException
import java.net.URL
import java.util.List
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.Nullable
import javax.net.SocketFactory
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam

/**
  * Tests Netty Router.
  */
object NettyRouterTestBase {
  protected val HOSTNAME: String = "127.0.0.1"
  protected val DISCOVERY_SERVICE: DiscoveryService = new InMemoryDiscoveryService
  protected val DEFAULT_SERVICE: String = Constants.Router.GATEWAY_DISCOVERY_NAME
  protected val APP_FABRIC_SERVICE: String = Constants.Service.APP_FABRIC_HTTP
  protected val CONNECTION_IDLE_TIMEOUT_SECS: Int = 2
  private val LOG: Logger = LoggerFactory.getLogger(classOf[NettyRouterTestBase])
  private val MAX_UPLOAD_BYTES: Int = 10 * 1024 * 1024
  private val CHUNK_SIZE: Int = 1024 * 1024

  private class ByteEntityWriter extends Request.EntityWriter {
    private final val bytes: Array[Byte] = null

    private def this(bytes: Array[Byte]) {
      this()
      this.bytes = bytes
    }

    @throws(classOf[IOException])
    def writeEntity(out: OutputStream) {
      {
        var i: Int = 0
        while (i < MAX_UPLOAD_BYTES) {
          {
            out.write(bytes, i, CHUNK_SIZE)
          }
          i += CHUNK_SIZE
        }
      }
    }
  }

  /**
    * A server for the router.
    */
  abstract class RouterService extends AbstractIdleService {
    def lookupService(serviceName: String): Int
  }

  /**
    * A generic server for testing router.
    */
  object ServerService {
    private val log: Logger = LoggerFactory.getLogger(classOf[NettyRouterTestBase.ServerService])
  }

  class ServerService extends AbstractIdleService {
    private final val hostname: String = null
    private final val discoveryService: DiscoveryService = null
    private final val serviceNameSupplier: Supplier[String] = null
    private final val numRequests: AtomicInteger = new AtomicInteger(0)
    private final val numConnectionsOpened: AtomicInteger = new AtomicInteger(0)
    private final val numConnectionsClosed: AtomicInteger = new AtomicInteger(0)
    private var httpService: NettyHttpService = null
    private var cancelDiscovery: Cancellable = null

    private def this(hostname: String, discoveryService: DiscoveryService, serviceNameSupplier: Supplier[String]) {
      this()
      this.hostname = hostname
      this.discoveryService = discoveryService
      this.serviceNameSupplier = serviceNameSupplier
    }

    protected def startUp {
      val builder: NettyHttpService.Builder = NettyHttpService.builder(classOf[NettyRouterTestBase.ServerService].getName)
      builder.addHttpHandlers(ImmutableSet.of(new NettyRouterTestBase.ServerService#ServerHandler))
      builder.setHost(hostname)
      builder.setPort(0)
      builder.modifyChannelPipeline(new Function[ChannelPipeline, ChannelPipeline]() {
        @Nullable def apply(input: ChannelPipeline): ChannelPipeline = {
          input.addLast("connection-counter", new SimpleChannelHandler() {
            @throws(classOf[Exception])
            override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
              numConnectionsOpened.incrementAndGet
              super.channelOpen(ctx, e)
            }

            @throws(classOf[Exception])
            override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
              numConnectionsClosed.incrementAndGet
              super.channelClosed(ctx, e)
            }
          })
          return input
        }
      })
      httpService = builder.build
      httpService.startAndWait
      registerServer
      ServerService.log.info("Started test server on {}", httpService.getBindAddress)
    }

    protected def shutDown {
      cancelDiscovery.cancel
      httpService.stopAndWait
    }

    def getNumRequests: Int = {
      return numRequests.get
    }

    def getNumConnectionsOpened: Int = {
      return numConnectionsOpened.get
    }

    def getNumConnectionsClosed: Int = {
      return numConnectionsClosed.get
    }

    def clearState {
      numRequests.set(0)
      numConnectionsOpened.set(0)
      numConnectionsClosed.set(0)
    }

    def registerServer {
      ServerService.log.info("Registering service {}", serviceNameSupplier.get)
      cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(new Discoverable(serviceNameSupplier.get, httpService.getBindAddress)))
    }

    def cancelRegistration {
      ServerService.log.info("Cancelling discovery registration of service {}", serviceNameSupplier.get)
      cancelDiscovery.cancel
    }

    /**
      * Simple handler for server.
      */
    class ServerHandler extends AbstractHttpHandler {
      private final val log: Logger = LoggerFactory.getLogger(classOf[NettyRouterTestBase.ServerService#ServerHandler])

      @GET
      @Path("/v1/echo/{text}") def echo(@SuppressWarnings(Array("UnusedParameters")) request: HttpRequest, responder: HttpResponder, @PathParam("text") text: String) {
        numRequests.incrementAndGet
        log.trace("Got text {}", text)
        responder.sendString(HttpResponseStatus.OK, text)
      }

      @GET
      @Path("/v1/ping/{text}") def ping(@SuppressWarnings(Array("UnusedParameters")) request: HttpRequest, responder: HttpResponder, @PathParam("text") text: String) {
        numRequests.incrementAndGet
        log.trace("Got text {}", text)
        responder.sendString(HttpResponseStatus.OK, serviceNameSupplier.get)
      }

      @GET
      @Path("/abc/v1/ping/{text}") def abcPing(@SuppressWarnings(Array("UnusedParameters")) request: HttpRequest, responder: HttpResponder, @PathParam("text") text: String) {
        numRequests.incrementAndGet
        log.trace("Got text {}", text)
        responder.sendString(HttpResponseStatus.OK, serviceNameSupplier.get)
      }

      @GET
      @Path("/def/v1/ping/{text}") def defPing(@SuppressWarnings(Array("UnusedParameters")) request: HttpRequest, responder: HttpResponder, @PathParam("text") text: String) {
        numRequests.incrementAndGet
        log.trace("Got text {}", text)
        responder.sendString(HttpResponseStatus.OK, serviceNameSupplier.get)
      }

      @GET
      @Path("/v2/ping") def gateway(@SuppressWarnings(Array("UnusedParameters")) request: HttpRequest, responder: HttpResponder) {
        numRequests.incrementAndGet
        responder.sendString(HttpResponseStatus.OK, serviceNameSupplier.get)
      }

      @GET
      @Path("/abc/v1/status") def abcStatus(request: HttpRequest, responder: HttpResponder) {
        numRequests.incrementAndGet
        responder.sendStatus(HttpResponseStatus.OK)
      }

      @GET
      @Path("/def/v1/status") def defStatus(request: HttpRequest, responder: HttpResponder) {
        numRequests.incrementAndGet
        responder.sendStatus(HttpResponseStatus.OK)
      }

      @GET
      @Path("/v1/timeout/{timeout-millis}")
      @throws(classOf[InterruptedException])
      def timeout(request: HttpRequest, responder: HttpResponder, @PathParam("timeout-millis") timeoutMillis: Int) {
        numRequests.incrementAndGet
        TimeUnit.MILLISECONDS.sleep(timeoutMillis)
        responder.sendStatus(HttpResponseStatus.OK)
      }

      @POST
      @Path("/v1/upload")
      @throws(classOf[IOException])
      def upload(request: HttpRequest, responder: HttpResponder) {
        val content: ChannelBuffer = request.getContent
        var readableBytes: Int = 0
        val chunkResponder: ChunkResponder = responder.sendChunkStart(HttpResponseStatus.OK, ImmutableMultimap.of[String, String])
        while ((({
          readableBytes = content.readableBytes; readableBytes
        })) > 0) {
          val read: Int = Math.min(readableBytes, CHUNK_SIZE)
          chunkResponder.sendChunk(content.readSlice(read))
        }
        chunkResponder.close
      }
    }

  }

}

abstract class NettyRouterTestBase {
  private final val defaultServiceSupplier: Supplier[String] = new Supplier[String]() {
    def get: String = {
      return NettyRouterTestBase.APP_FABRIC_SERVICE
    }
  }
  final val routerService: NettyRouterTestBase.RouterService = createRouterService
  final val defaultServer1: NettyRouterTestBase.ServerService = new NettyRouterTestBase.ServerService(NettyRouterTestBase.HOSTNAME, NettyRouterTestBase.DISCOVERY_SERVICE, defaultServiceSupplier)
  final val defaultServer2: NettyRouterTestBase.ServerService = new NettyRouterTestBase.ServerService(NettyRouterTestBase.HOSTNAME, NettyRouterTestBase.DISCOVERY_SERVICE, defaultServiceSupplier)
  final val allServers: util.List[NettyRouterTestBase.ServerService] = Lists.newArrayList(defaultServer1, defaultServer2)

  protected def createRouterService: NettyRouterTestBase.RouterService

  protected def getProtocol: String

  @throws(classOf[Exception])
  protected def getHTTPClient: DefaultHttpClient

  @throws(classOf[Exception])
  protected def getSocketFactory: SocketFactory

  protected def lookupService(serviceName: String): Int = {
    return routerService.lookupService(serviceName)
  }

  @throws(classOf[URISyntaxException])
  private def resolveURI(serviceName: String, path: String): String = {
    return getBaseURI(serviceName).resolve(path).toASCIIString
  }

  @throws(classOf[URISyntaxException])
  private def getBaseURI(serviceName: String): URI = {
    val servicePort: Int = lookupService(serviceName)
    return new URI(String.format("%s://%s:%d", getProtocol, NettyRouterTestBase.HOSTNAME, servicePort))
  }

  @Before
  @throws(classOf[Exception])
  def startUp {
    routerService.startAndWait
    import scala.collection.JavaConversions._
    for (server <- allServers) {
      server.clearState
      server.startAndWait
    }
    val discoverables: Iterable[Discoverable] = (NettyRouterTestBase.DISCOVERY_SERVICE.asInstanceOf[DiscoveryServiceClient]).discover(defaultServiceSupplier.get)
    {
      var i: Int = 0
      while (i < 50 && Iterables.size(discoverables) != 2) {
        {
          TimeUnit.MILLISECONDS.sleep(50)
        }
        ({
          i += 1; i
        })
      }
    }
  }

  @After def tearDown {
    import scala.collection.JavaConversions._
    for (server <- allServers) {
      server.stopAndWait
    }
    routerService.stopAndWait
  }

  @Test
  @throws(classOf[Exception])
  def testRouterSync {
    testSync(25)
    Assert.assertEquals(25, defaultServer1.getNumRequests + defaultServer2.getNumRequests)
  }

  @Test
  @throws(classOf[Exception])
  def testRouterAsync {
    val numElements: Int = 123
    val configBuilder: AsyncHttpClientConfig.Builder = new AsyncHttpClientConfig.Builder
    val asyncHttpClient: AsyncHttpClient = new AsyncHttpClient(new NettyAsyncHttpProvider(configBuilder.build), configBuilder.build)
    val latch: CountDownLatch = new CountDownLatch(numElements)
    val numSuccessfulRequests: AtomicInteger = new AtomicInteger(0)
    {
      var i: Int = 0
      while (i < numElements) {
        {
          val elem: Int = i
          val request: Request = new RequestBuilder("GET").setUrl(resolveURI(NettyRouterTestBase.DEFAULT_SERVICE, String.format("%s/%s-%d", "/v1/echo", "async", i))).build
          asyncHttpClient.executeRequest(request, new AsyncCompletionHandler[Void]() {
            @throws(classOf[Exception])
            def onCompleted(response: Response): Void = {
              latch.countDown
              Assert.assertEquals(HttpResponseStatus.OK.getCode, response.getStatusCode)
              val responseBody: String = response.getResponseBody
              NettyRouterTestBase.LOG.trace("Got response {}", responseBody)
              Assert.assertEquals("async-" + elem, responseBody)
              numSuccessfulRequests.incrementAndGet
              return null
            }

            override def onThrowable(t: Throwable) {
              NettyRouterTestBase.LOG.error("Got exception while posting {}", elem, t)
              latch.countDown
            }
          })
          TimeUnit.MILLISECONDS.sleep(1)
        }
        ({
          i += 1; i
        })
      }
    }
    latch.await
    asyncHttpClient.close
    Assert.assertEquals(numElements, numSuccessfulRequests.get)
    Assert.assertTrue(numElements == (defaultServer1.getNumRequests + defaultServer2.getNumRequests))
  }

  @Test
  @throws(classOf[Exception])
  def testRouterOneServerDown {
    try {
      defaultServer1.cancelRegistration
      testSync(25)
    } finally {
      Assert.assertEquals(0, defaultServer1.getNumRequests)
      Assert.assertTrue(defaultServer2.getNumRequests > 0)
      defaultServer1.registerServer
    }
  }

  @Test
  @throws(classOf[Exception])
  def testRouterAllServersDown {
    try {
      defaultServer1.cancelRegistration
      defaultServer2.cancelRegistration
      testSyncServiceUnavailable
    } finally {
      Assert.assertEquals(0, defaultServer1.getNumRequests)
      Assert.assertEquals(0, defaultServer2.getNumRequests)
      defaultServer1.registerServer
      defaultServer2.registerServer
    }
  }

  @Test
  @throws(classOf[Exception])
  def testHostForward {
    val response: HttpResponse = get(resolveURI(NettyRouterTestBase.DEFAULT_SERVICE, String.format("%s/%s", "/v1/ping", "sync")))
    Assert.assertEquals(HttpResponseStatus.OK.getCode, response.getStatusLine.getStatusCode)
    Assert.assertEquals(defaultServiceSupplier.get, EntityUtils.toString(response.getEntity))
  }

  @Test
  @throws(classOf[Exception])
  def testUpload {
    val configBuilder: AsyncHttpClientConfig.Builder = new AsyncHttpClientConfig.Builder
    val asyncHttpClient: AsyncHttpClient = new AsyncHttpClient(new NettyAsyncHttpProvider(configBuilder.build), configBuilder.build)
    val requestBody: Array[Byte] = generatePostData
    val request: Request = new RequestBuilder("POST").setUrl(resolveURI(NettyRouterTestBase.DEFAULT_SERVICE, "/v1/upload")).setContentLength(requestBody.length).setBody(new NettyRouterTestBase.ByteEntityWriter(requestBody)).build
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    val future: Future[Void] = asyncHttpClient.executeRequest(request, new AsyncCompletionHandler[Void]() {
      @throws(classOf[Exception])
      def onCompleted(response: Response): Void = {
        return null
      }

      @throws(classOf[Exception])
      override def onBodyPartReceived(content: HttpResponseBodyPart): AsyncHandler.STATE = {
        content.writeTo(byteArrayOutputStream)
        return super.onBodyPartReceived(content)
      }
    })
    future.get
    Assert.assertArrayEquals(requestBody, byteArrayOutputStream.toByteArray)
  }

  @Test
  @throws(classOf[Exception])
  def testConnectionClose {
    val urls: Array[URL] = Array[URL](new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/abc/v1/status")), new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/def/v1/status")))
    val times: Int = 1000
    var keepAlive: Boolean = true
    {
      var i: Int = 0
      while (i < times) {
        {
          val urlConn: HttpURLConnection = openURL(urls(i % urls.length))
          try {
            urlConn.setRequestProperty(HttpHeaders.Names.CONNECTION, if (keepAlive) HttpHeaders.Values.KEEP_ALIVE else HttpHeaders.Values.CLOSE)
            Assert.assertEquals(HttpURLConnection.HTTP_OK, urlConn.getResponseCode)
          } finally {
            keepAlive = !keepAlive
            urlConn.disconnect
          }
        }
        ({
          i += 1; i - 1
        })
      }
    }
    Assert.assertEquals(times, defaultServer1.getNumRequests + defaultServer2.getNumRequests)
  }

  @Test(timeout = 10000)
  @throws(classOf[Exception])
  def testConnectionIdleTimeout {
    defaultServer2.cancelRegistration
    val path: String = "/v2/ping"
    val uri: URI = new URI(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, path))
    val socket: Socket = getSocketFactory.createSocket(uri.getHost, uri.getPort)
    val out: PrintWriter = new PrintWriter(socket.getOutputStream, true)
    val inputStream: InputStream = socket.getInputStream
    var firstLine: String = makeRequest(uri, out, inputStream)
    Assert.assertEquals("HTTP/1.1 200 OK\r", firstLine)
    TimeUnit.MILLISECONDS.sleep(TimeUnit.SECONDS.toMillis(NettyRouterTestBase.CONNECTION_IDLE_TIMEOUT_SECS) - 500)
    firstLine = makeRequest(uri, out, inputStream)
    Assert.assertEquals("HTTP/1.1 200 OK\r", firstLine)
    TimeUnit.MILLISECONDS.sleep(TimeUnit.SECONDS.toMillis(NettyRouterTestBase.CONNECTION_IDLE_TIMEOUT_SECS) + 500)
    makeRequest(uri, out, inputStream)
    Assert.assertEquals(2, defaultServer1.getNumRequests + defaultServer2.getNumRequests)
    Assert.assertEquals(1, defaultServer1.getNumConnectionsOpened + defaultServer2.getNumConnectionsOpened)
    Assert.assertEquals(1, defaultServer1.getNumConnectionsClosed + defaultServer2.getNumConnectionsClosed)
  }

  @throws(classOf[IOException])
  private def makeRequest(uri: URI, out: PrintWriter, inputStream: InputStream): String = {
    out.print("GET " + uri.getPath + " HTTP/1.1\r\n" + "Host: " + uri.getHost + "\r\n" + "Connection: keep-alive\r\n\r\n")
    out.flush
    val buffer: Array[Byte] = new Array[Byte](1024)
    var length: Int = 0
    {
      var i: Int = 0
      while (i < 20) {
        {
          val read: Int = inputStream.read(buffer, length, 1024 - length)
          length += if (read < 0) 0 else read
          if (length >= 108) {
            break //todo: break is not supported
          }
        }
        ({
          i += 1; i
        })
      }
    }
    return Iterables.getFirst(Splitter.on("\n").split(new String(buffer, Charsets.UTF_8.name)), "")
  }

  @Test
  @throws(classOf[Exception])
  def testConnectionIdleTimeoutWithMultipleServers {
    defaultServer2.cancelRegistration
    var url: URL = new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v2/ping"))
    var urlConnection: HttpURLConnection = openURL(url)
    Assert.assertEquals(200, urlConnection.getResponseCode)
    urlConnection.getInputStream.close
    urlConnection.disconnect
    defaultServer1.cancelRegistration
    defaultServer2.registerServer
    {
      var i: Int = 0
      while (i < 4) {
        {
          TimeUnit.SECONDS.sleep(1)
          url = new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v1/ping/" + i))
          urlConnection = openURL(url)
          Assert.assertEquals(200, urlConnection.getResponseCode)
          urlConnection.getInputStream.close
          urlConnection.disconnect
        }
        ({
          i += 1; i - 1
        })
      }
    }
    Assert.assertEquals(1, defaultServer1.getNumConnectionsOpened)
    Assert.assertEquals(1, defaultServer1.getNumConnectionsClosed)
    Assert.assertEquals(1, defaultServer2.getNumConnectionsOpened)
    Assert.assertEquals(0, defaultServer2.getNumConnectionsClosed)
    defaultServer2.registerServer
    defaultServer1.cancelRegistration
    url = new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v2/ping"))
    urlConnection = openURL(url)
    Assert.assertEquals(200, urlConnection.getResponseCode)
    urlConnection.getInputStream.close
    urlConnection.disconnect
  }

  @Test
  @throws(classOf[Exception])
  def testConnectionNoIdleTimeout {
    val timeoutMillis: Long = TimeUnit.SECONDS.toMillis(NettyRouterTestBase.CONNECTION_IDLE_TIMEOUT_SECS) + 500
    val url: URL = new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v1/timeout/" + timeoutMillis))
    val urlConnection: HttpURLConnection = openURL(url)
    Assert.assertEquals(200, urlConnection.getResponseCode)
    urlConnection.disconnect
  }

  @throws(classOf[Exception])
  protected def openURL(url: URL): HttpURLConnection = {
    return url.openConnection.asInstanceOf[HttpURLConnection]
  }

  @throws(classOf[Exception])
  private def testSync(numRequests: Int) {
    {
      var i: Int = 0
      while (i < numRequests) {
        {
          NettyRouterTestBase.LOG.trace("Sending request " + i)
          val response: HttpResponse = get(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, String.format("%s/%s-%d", "/v1/ping", "sync", i)))
          Assert.assertEquals(HttpResponseStatus.OK.getCode, response.getStatusLine.getStatusCode)
        }
        ({
          i += 1; i
        })
      }
    }
  }

  @throws(classOf[Exception])
  private def testSyncServiceUnavailable {
    {
      var i: Int = 0
      while (i < 25) {
        {
          NettyRouterTestBase.LOG.trace("Sending request " + i)
          val response: HttpResponse = get(resolveURI(NettyRouterTestBase.DEFAULT_SERVICE, String.format("%s/%s-%d", "/v1/ping", "sync", i)))
          Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode, response.getStatusLine.getStatusCode)
        }
        ({
          i += 1; i
        })
      }
    }
  }

  private def generatePostData: Array[Byte] = {
    val bytes: Array[Byte] = new Array[Byte](NettyRouterTestBase.MAX_UPLOAD_BYTES)
    {
      var i: Int = 0
      while (i < NettyRouterTestBase.MAX_UPLOAD_BYTES) {
        {
          bytes(i) = i.toByte
        }
        ({
          i += 1; i
        })
      }
    }
    return bytes
  }

  @throws(classOf[Exception])
  private def get(url: String): HttpResponse = {
    return get(url, null)
  }

  @throws(classOf[Exception])
  private def get(url: String, headers: Array[Header]): HttpResponse = {
    val client: DefaultHttpClient = getHTTPClient
    val get: HttpGet = new HttpGet(url)
    if (headers != null) {
      get.setHeaders(headers)
    }
    return client.execute(get)
  }
}
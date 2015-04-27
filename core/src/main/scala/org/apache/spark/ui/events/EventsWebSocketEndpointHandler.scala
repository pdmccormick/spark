package org.apache.spark.ui.events

import java.io.IOException
import java.util.concurrent.CopyOnWriteArraySet
import javax.servlet.ServletException
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.spark.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.websocket.{WebSocket, WebSocketFactory}
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._

class EventsWebSocketEndpointHandler(
    val prefix : String,
    val listener : EventsBusListener)
  extends ServletContextHandler
  with WebSocketFactory.Acceptor
  with Logging {
  val factory : WebSocketFactory = new WebSocketFactory(this, 32 * 1024)
  val clients = new CopyOnWriteArraySet[EventsWebSocketClientHandler]

  def getWebSocketFactory: WebSocketFactory = factory

  setContextPath(prefix)
  listener.addWSHandler(this)

  @throws (classOf[IOException] )
  @throws (classOf[ServletException] )
  override def doHandle (target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) {
    if (factory.acceptWebSocket(request, response) || response.isCommitted) {
      baseRequest.setHandled(true)
      return
    }

    super.handle (target, baseRequest, request, response)
  }

  def checkOrigin (request: HttpServletRequest, origin: String): Boolean = {
    true
  }

  override def doWebSocketConnect(request: HttpServletRequest, protocol: String) : WebSocket = {
    logDebug("doWebSocketConnect: " + protocol)
    val client = new EventsWebSocketClientHandler(this)
    clients.add(client)

    client
  }

  def broadcastEvent(event: SparkListenerEvent) = {
    val repr = pretty(JsonProtocol.sparkEventToJson(event))

    logDebug("broadcasting event: " + event)

    for (client <- clients if client.connection.isOpen) {
      logDebug("      broadcast => " + client.connection)
      client.connection.sendMessage(repr)
    }
  }
}

package org.apache.spark.ui

import scala.collection.JavaConversions._

import java.io.IOException
import java.util.concurrent.CopyOnWriteArraySet
import javax.servlet.ServletException
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.util.TypeUtil
import org.eclipse.jetty.websocket.WebSocket.{FrameConnection, Connection}
import org.eclipse.jetty.websocket.{WebSocketFactory, WebSocket, WebSocketHandler}

import org.json4s.jackson.JsonMethods.pretty

@DeveloperApi
class WebSocketClientHandler(scope : WebSocketUIHandler) extends WebSocket
    with WebSocket.OnFrame
    with WebSocket.OnBinaryMessage
    with WebSocket.OnTextMessage
    with WebSocket.OnControl
    with Logging {

  private[spark] var conn : FrameConnection = null

  def getConnection() : FrameConnection = conn

  override def onOpen(connection: Connection): Unit = {
    logDebug("onOpen " + conn + " " + conn.getProtocol())
  }

  override def onClose(closeCode: Int, message: String): Unit = {
    logDebug("onClose " + closeCode + " " + message)
    scope.clients.remove(this)
  }

  override def onFrame(flags: Byte, opcode: Byte, data: Array[Byte], offset: Int, length: Int): Boolean = {
    logDebug("onFrame " + TypeUtil.toHexString(flags) + " " + TypeUtil.toHexString(opcode) + " " + TypeUtil.toHexString(data,offset,length))
    false
  }

  override def onHandshake(connection: FrameConnection): Unit = {
    conn = connection
    logDebug("onHandshake " + conn + " " + conn.getClass().getSimpleName())
  }

  override def onMessage(data: Array[Byte], offset: Int, length: Int): Unit = {
    logDebug("onMessage " + TypeUtil.toHexString(data,offset,length))
  }

  override def onMessage(data: String): Unit = {
    logDebug("onMessage " + data)

    conn.sendMessage("replying to: " + data)
  }

  override def onControl(controlCode: Byte, data: Array[Byte], offset: Int, length: Int): Boolean = {
    logDebug("onControl " + TypeUtil.toHexString(controlCode) + " " + TypeUtil.toHexString(data,offset,length))
    false
  }
}

//class WebSocketUIHandler extends HandlerWrapper with WebSocketFactory.Acceptor with Logging {
class WebSocketUIHandler(listener : WebSocketListener) extends ServletContextHandler with WebSocketFactory.Acceptor with Logging {
  val factory : WebSocketFactory = new WebSocketFactory(this, 32 * 1024)
  val clients = new CopyOnWriteArraySet[WebSocketClientHandler]

  def getWebSocketFactory: WebSocketFactory = factory

  listener.addUIHandler(this)

  @throws (classOf[IOException] )
  @throws (classOf[ServletException] )
  override def doHandle (target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) {
    response.setHeader("Access-Control-Allow-Origin", "*")

    if (factory.acceptWebSocket(request, response) || response.isCommitted) {
      baseRequest.setHandled(true)
      return
    }

    super.handle (target, baseRequest, request, response)
  }

  def checkOrigin (request: HttpServletRequest, origin: String): Boolean = {
    return true
  }

  override def doWebSocketConnect(request: HttpServletRequest, protocol: String) : WebSocket = {
    logDebug("doWebSocketConnect: " + protocol)
    val client = new WebSocketClientHandler(this)
    clients.add(client)

    client
  }

  def broadcastEvent(event: SparkListenerEvent) = {
    val repr = pretty(JsonProtocol.sparkEventToJson(event))

    logDebug("broadcasting event: " + event)

    for (client <- clients if client.conn.isOpen) {
      logDebug("      broadcast => " + client.conn)
      client.conn.sendMessage(repr)
    }
  }

}

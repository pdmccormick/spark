package org.apache.spark.ui.events

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.eclipse.jetty.util.TypeUtil
import org.eclipse.jetty.websocket.WebSocket
import org.eclipse.jetty.websocket.WebSocket.{Connection, FrameConnection}

@DeveloperApi
class EventsWebSocketClientHandler(
      val scope : EventsWebSocketEndpointHandler)
    extends WebSocket
    with WebSocket.OnFrame
    with WebSocket.OnBinaryMessage
    with WebSocket.OnTextMessage
    with WebSocket.OnControl
    with Logging {

  private[spark] var connection : FrameConnection = null

  override def onOpen(connection_ : Connection): Unit = {
    logDebug("onOpen " + connection + " " + connection.getProtocol)
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
    this.connection = connection
    logDebug("onHandshake " + connection + " " + connection.getClass.getSimpleName)
  }

  override def onMessage(data: Array[Byte], offset: Int, length: Int): Unit = {
    logDebug("onMessage " + TypeUtil.toHexString(data,offset,length))
  }

  override def onMessage(data: String): Unit = {
    logDebug("onMessage " + data)
  }

  override def onControl(controlCode: Byte, data: Array[Byte], offset: Int, length: Int): Boolean = {
    logDebug("onControl " + TypeUtil.toHexString(controlCode) + " " + TypeUtil.toHexString(data,offset,length))
    false
  }
}

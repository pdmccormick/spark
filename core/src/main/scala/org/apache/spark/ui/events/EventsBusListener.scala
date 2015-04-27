package org.apache.spark.ui.events

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._

@DeveloperApi
class EventsBusListener extends SparkListener with Logging {
  var wsHandler : Option[EventsWebSocketEndpointHandler] = None

  def addWSHandler(handler: EventsWebSocketEndpointHandler) = {
    wsHandler = Some(handler)
  }

  def postEvent(event: SparkListenerEvent) = wsHandler match {
      case Some(handler) => handler.broadcastEvent(event)
      case _ =>
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = postEvent(stageCompleted)

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = postEvent(stageSubmitted)

  override def onTaskStart(taskStart: SparkListenerTaskStart) = postEvent(taskStart)

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) = postEvent(taskGettingResult)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = postEvent(taskEnd)

  override def onJobStart(jobStart: SparkListenerJobStart) = postEvent(jobStart)

  override def onJobEnd(jobEnd: SparkListenerJobEnd) = postEvent(jobEnd)

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) = postEvent(environmentUpdate)

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) = postEvent(blockManagerAdded)

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) = postEvent(blockManagerRemoved)

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) = postEvent(unpersistRDD)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) = postEvent(applicationStart)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) = postEvent(applicationEnd)

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) = postEvent(executorMetricsUpdate)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) = postEvent(executorAdded)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) = postEvent(executorRemoved)
}

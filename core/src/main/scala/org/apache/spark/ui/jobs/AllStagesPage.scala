/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, NodeSeq}

import org.apache.spark.scheduler.Schedulable
import org.apache.spark.ui.{WebUIPage, UIUtils}

/** Page showing list of all ongoing and recently finished stages and pools */
private[ui] class AllStagesPage(parent: StagesTab) extends WebUIPage("") {
  private val sc = parent.sc
  private val listener = parent.listener
  private def isFairScheduler = parent.isFairScheduler

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val activeStages = listener.activeStages.values.toSeq
      val pendingStages = listener.pendingStages.values.toSeq
      val completedStages = listener.completedStages.reverse.toSeq
      val numCompletedStages = listener.numCompletedStages
      val failedStages = listener.failedStages.reverse.toSeq
      val numFailedStages = listener.numFailedStages
      val now = System.currentTimeMillis

      val activeStagesTable =
        new StageTableBase(activeStages.sortBy(_.submissionTime).reverse,
          parent.basePath, parent.listener, isFairScheduler = parent.isFairScheduler,
          killEnabled = parent.killEnabled)
      val pendingStagesTable =
        new StageTableBase(pendingStages.sortBy(_.submissionTime).reverse,
          parent.basePath, parent.listener, isFairScheduler = parent.isFairScheduler,
          killEnabled = false)
      val completedStagesTable =
        new StageTableBase(completedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.listener, isFairScheduler = parent.isFairScheduler, killEnabled = false)
      val failedStagesTable =
        new FailedStageTable(failedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.listener, isFairScheduler = parent.isFairScheduler)

      // For now, pool information is only accessible in live UIs
      val pools = sc.map(_.getAllPools).getOrElse(Seq.empty[Schedulable])
      val poolTable = new PoolTable(pools, parent)

      val shouldShowActiveStages = activeStages.nonEmpty
      val shouldShowPendingStages = pendingStages.nonEmpty
      val shouldShowCompletedStages = completedStages.nonEmpty
      val shouldShowFailedStages = failedStages.nonEmpty

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {
              if (sc.isDefined) {
                // Total duration is not meaningful unless the UI is live
                <li>
                  <strong>Total Duration: </strong>
                  <span class="total-duration">{UIUtils.formatDuration(now - sc.get.startTime)}</span>
                </li>
              }
            }
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
            </li>
            {
              if (shouldShowActiveStages) {
                <li>
                  <a href="#active"><strong>Active Stages:</strong></a>
                  <span class="active-stages-count">{activeStages.size}</span>
                </li>
              }
            }
            {
              if (shouldShowPendingStages) {
                <li>
                  <a href="#pending"><strong>Pending Stages:</strong></a>
                  <span class="pending-stages-count">{pendingStages.size}</span>
                </li>
              }
            }
            {
              if (shouldShowCompletedStages) {
                <li>
                  <a href="#completed"><strong>Completed Stages:</strong></a>
                  <span class="completed-stages-count">{numCompletedStages}</span>
                </li>
              }
            }
            {
              if (shouldShowFailedStages) {
                <li>
                  <a href="#failed"><strong>Failed Stages:</strong></a>
                  <span class="failed-stages-count">{numFailedStages}</span>
                </li>
              }
            }
          </ul>
        </div>

      var content = summary ++
        {
          if (sc.isDefined && isFairScheduler) {
            <div id="scheduler-pools">
              <h4><span class="scheduler-pool-count">{pools.size}</span> Fair Scheduler Pools</h4>
              { poolTable.toNodeSeq }
            </div>
          } else {
            Seq[Node]()
          }
        }
      if (shouldShowActiveStages) {
        content ++=
          <div id="active-stages">
            <h4 id="active">Active Stages (<span class="active-stages-count">{activeStages.size}</span>)</h4>
            { activeStagesTable.toNodeSeq }
          </div>
      }
      if (shouldShowPendingStages) {
        content ++=
          <div id="pending-stages">
            <h4 id="pending">Pending Stages (<span class="pending-stages-count">{pendingStages.size}</span>)</h4>
            { pendingStagesTable.toNodeSeq }
          </div>
      }
      if (shouldShowCompletedStages) {
        content ++=
          <div id="completed-stages">
            <h4 id="completed">Completed Stages (<span class="completed-stages-count">{numCompletedStages}</span>)</h4>
            { completedStagesTable.toNodeSeq }
          </div>
      }
      if (shouldShowFailedStages) {
        content ++=
          <div id="failed-stages">
            <h4 id ="failed">Failed Stages (<span class="failed-stages-count">{numFailedStages}</span>)</h4>
            { failedStagesTable.toNodeSeq }
          </div>
      }
      UIUtils.headerSparkPage("Spark Stages (for all jobs)", content, parent)
    }
  }
}

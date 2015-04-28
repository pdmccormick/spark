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

import scala.collection.mutable
import scala.xml.{NodeSeq, Node}

import javax.servlet.http.HttpServletRequest

import org.apache.spark.JobExecutionStatus
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui.{UIUtils, WebUIPage}

/** Page showing statistics and stage list for a given job */
private[ui] class JobPage(parent: JobsTab) extends WebUIPage("job") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

      val jobId = parameterId.toInt
      val jobDataOption = listener.jobIdToData.get(jobId)
      if (jobDataOption.isEmpty) {
        val content =
          <div>
            <p>No information to display for job {jobId}</p>
          </div>
        return UIUtils.headerSparkPage(
          s"Details for Job $jobId", content, parent)
      }
      val jobData = jobDataOption.get
      val isComplete = jobData.status != JobExecutionStatus.RUNNING
      val stages = jobData.stageIds.map { stageId =>
        // This could be empty if the JobProgressListener hasn't received information about the
        // stage or if the stage information has been garbage collected
        listener.stageIdToInfo.getOrElse(stageId,
          new StageInfo(stageId, 0, "Unknown", 0, Seq.empty, "Unknown"))
      }

      val activeStages = mutable.Buffer[StageInfo]()
      val completedStages = mutable.Buffer[StageInfo]()
      // If the job is completed, then any pending stages are displayed as "skipped":
      val pendingOrSkippedStages = mutable.Buffer[StageInfo]()
      val failedStages = mutable.Buffer[StageInfo]()
      for (stage <- stages) {
        if (stage.submissionTime.isEmpty) {
          pendingOrSkippedStages += stage
        } else if (stage.completionTime.isDefined) {
          if (stage.failureReason.isDefined) {
            failedStages += stage
          } else {
            completedStages += stage
          }
        } else {
          activeStages += stage
        }
      }

      val activeStagesTable =
        new StageTableBase(activeStages.sortBy(_.submissionTime).reverse,
          parent.basePath, parent.listener, isFairScheduler = parent.isFairScheduler,
          killEnabled = parent.killEnabled)
      val pendingOrSkippedStagesTable =
        new StageTableBase(pendingOrSkippedStages.sortBy(_.stageId).reverse,
          parent.basePath, parent.listener, isFairScheduler = parent.isFairScheduler,
          killEnabled = false)
      val completedStagesTable =
        new StageTableBase(completedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.listener, isFairScheduler = parent.isFairScheduler, killEnabled = false)
      val failedStagesTable =
        new FailedStageTable(failedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.listener, isFairScheduler = parent.isFairScheduler)

      val shouldShowActiveStages = activeStages.nonEmpty
      val shouldShowPendingStages = !isComplete && pendingOrSkippedStages.nonEmpty
      val shouldShowCompletedStages = completedStages.nonEmpty
      val shouldShowSkippedStages = isComplete && pendingOrSkippedStages.nonEmpty
      val shouldShowFailedStages = failedStages.nonEmpty

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            <li>
              <Strong>Status:</Strong>
              <span class="job-status">{jobData.status}</span>
            </li>
            {
              if (jobData.jobGroup.isDefined) {
                <li>
                  <strong>Job Group:</strong>
                  {jobData.jobGroup.get}
                </li>
              }
            }
            {
              if (shouldShowActiveStages) {
                <li>
                  <a href="#active"><strong>Active Stages:</strong></a>
                  <span class="active-stage-count">{activeStages.size}</span>
                </li>
              }
            }
            {
              if (shouldShowPendingStages) {
                <li>
                  <a href="#pending">
                    <strong>Pending Stages:</strong>
                  </a><span class="pending-stage-count">{pendingOrSkippedStages.size}</span>
                </li>
              }
            }
            {
              if (shouldShowCompletedStages) {
                <li>
                  <a href="#completed"><strong>Completed Stages:</strong></a>
                  <span class="completed-stage-count">{completedStages.size}</span>
                </li>
              }
            }
            {
              if (shouldShowSkippedStages) {
              <li>
                <a href="#skipped"><strong>Skipped Stages:</strong></a>
                <span class="skipped-stage-count">{pendingOrSkippedStages.size}</span>
              </li>
            }
            }
            {
              if (shouldShowFailedStages) {
                <li>
                  <a href="#failed"><strong>Failed Stages:</strong></a>
                  <span class="failed-stage-count">{failedStages.size}</span>
                </li>
              }
            }
          </ul>
        </div>

      var content = summary
      if (shouldShowActiveStages) {
        content ++=
          <div id="active-stages">
            <h4 id="active">Active Stages (<span class="active-stage-count">{activeStages.size}</span>)</h4>
            { activeStagesTable.toNodeSeq }
          </div>
      }
      if (shouldShowPendingStages) {
        content ++=
          <div id="pending-stages">
            <h4 id="pending">Pending Stages (<span class="pending-stage-count">{pendingOrSkippedStages.size}</span>)</h4>
            { pendingOrSkippedStagesTable.toNodeSeq }
          </div>
      }
      if (shouldShowCompletedStages) {
        content ++=
          <div id="completed-stages">
            <h4 id="completed">Completed Stages (<span class="completed-stage-count">{completedStages.size}</span>)</h4>
            { completedStagesTable.toNodeSeq }
          </div>
      }
      if (shouldShowSkippedStages) {
        content ++=
          <div id="skipped-stages">
            <h4 id="skipped">Skipped Stages (<span class="skipped-stage-count">{pendingOrSkippedStages.size}</span>)</h4>
            { pendingOrSkippedStagesTable.toNodeSeq }
          </div>
      }
      if (shouldShowFailedStages) {
        content ++=
          <div id="failed-stages">
            <h4 id ="failed">Failed Stages (<span class="failed-stage-count">{failedStages.size}</span>)</h4>
            { failedStagesTable.toNodeSeq }
          </div>
      }
      UIUtils.headerSparkPage(s"Details for Job $jobId", content, parent)
    }
  }
}

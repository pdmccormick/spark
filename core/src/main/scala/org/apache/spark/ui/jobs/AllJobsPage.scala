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

import scala.xml.{Node, NodeSeq}

import javax.servlet.http.HttpServletRequest

import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.ui.jobs.UIData.JobUIData

/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPage(parent: JobsTab) extends WebUIPage("") {
  private val startTime: Option[Long] = parent.sc.map(_.startTime)
  private val listener = parent.listener

  private def jobsTable(jobs: Seq[JobUIData]): Seq[Node] = {
    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)

    val columns: Seq[Node] = {
      <th>{if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"}</th>
      <th>Description</th>
      <th>Submitted</th>
      <th>Duration</th>
      <th class="sorttable_nosort">Stages: Succeeded/Total</th>
      <th class="sorttable_nosort">Tasks (for all stages): Succeeded/Total</th>
    }

    def makeRow(job: JobUIData): Seq[Node] = {
      val lastStageInfo = Option(job.stageIds)
        .filter(_.nonEmpty)
        .flatMap { ids => listener.stageIdToInfo.get(ids.max) }
      val lastStageData = lastStageInfo.flatMap { s =>
        listener.stageIdToData.get((s.stageId, s.attemptId))
      }

      val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
      val lastStageDescription = lastStageData.flatMap(_.description).getOrElse("")
      val duration: Option[Long] = {
        job.submissionTime.map { start =>
          val end = job.completionTime.getOrElse(System.currentTimeMillis())
          end - start
        }
      }
      val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
      val formattedSubmissionTime = job.submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
      val detailUrl =
        "%s/jobs/job?id=%s".format(UIUtils.prependBaseUri(parent.basePath), job.jobId)
      <tr id={ "job-" + job.jobId.toString }>
        <td sorttable_customkey={job.jobId.toString} class="job-and-group-id-field">
          {job.jobId} {job.jobGroup.map(id => s"($id)").getOrElse("")}
        </td>
        <td class="job-description">
          <span class="description-input" title={lastStageDescription}>{lastStageDescription}</span>
          <a href={detailUrl}>{lastStageName}</a>
        </td>
        <td sorttable_customkey={job.submissionTime.getOrElse(-1).toString} class="job-submitted">
          {formattedSubmissionTime}
        </td>
        <td sorttable_customkey={duration.getOrElse(-1).toString} class="job-duration">{formattedDuration}</td>
        <td class="stage-progress-cell job-stage-progress">
          <span class="completed-job-stage-count">{job.completedStageIndices.size}</span>/<span class="total-job-stage-count">{job.stageIds.size - job.numSkippedStages}</span>
          {if (job.numFailedStages > 0) { "(" ++ <span class="failed-job-stage-count">{ job.numFailedStages.toString }</span> ++ " failed)" }}
          {if (job.numSkippedStages > 0) { "(" ++ <span class="skipped-job-stage-count">{ job.numSkippedStages.toString }</span> ++ " skipped)" }}
        </td>
        <td class="progress-cell job-task-progress">
          {UIUtils.makeProgressBar(started = job.numActiveTasks, completed = job.numCompletedTasks,
           failed = job.numFailedTasks, skipped = job.numSkippedTasks,
           total = job.numTasks - job.numSkippedTasks)}
        </td>
      </tr>
    }

    <table class="table table-bordered table-striped table-condensed sortable jobs-table">
      <thead>{columns}</thead>
      <tbody>
        {jobs.map(makeRow)}
      </tbody>
    </table>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val activeJobs = listener.activeJobs.values.toSeq
      val completedJobs = listener.completedJobs.reverse.toSeq
      val failedJobs = listener.failedJobs.reverse.toSeq
      val now = System.currentTimeMillis

      val activeJobsTable =
        jobsTable(activeJobs.sortBy(_.submissionTime.getOrElse(-1L)).reverse)
      val completedJobsTable =
        jobsTable(completedJobs.sortBy(_.completionTime.getOrElse(-1L)).reverse)
      val failedJobsTable =
        jobsTable(failedJobs.sortBy(_.completionTime.getOrElse(-1L)).reverse)

      val shouldShowActiveJobs = activeJobs.nonEmpty
      val shouldShowCompletedJobs = completedJobs.nonEmpty
      val shouldShowFailedJobs = failedJobs.nonEmpty

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {if (startTime.isDefined) {
              // Total duration is not meaningful unless the UI is live
              <li>
                <strong>Total Duration: </strong>
                <span class="total-duration">{UIUtils.formatDuration(now - startTime.get)}</span>
              </li>
            }}
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
            </li>
            {
              if (shouldShowActiveJobs) {
                <li>
                  <a href="#active"><strong>Active Jobs:</strong></a>
                  <span class="active-job-count">{activeJobs.size}</span>
                </li>
              }
            }
            {
              if (shouldShowCompletedJobs) {
                <li>
                  <a href="#completed"><strong>Completed Jobs:</strong></a>
                  <span class="completed-job-count">{completedJobs.size}</span>
                </li>
              }
            }
            {
              if (shouldShowFailedJobs) {
                <li>
                  <a href="#failed"><strong>Failed Jobs:</strong></a>
                  <span class="failed-job-count">{failedJobs.size}</span>
                </li>
              }
            }
          </ul>
        </div>

      var content = summary
      if (shouldShowActiveJobs) {
        content ++=
          <div id="active-jobs">
            <h4 id="active">Active Jobs (<span class="active-job-count">{activeJobs.size}</span>)</h4>
            { activeJobsTable }
          </div>
      }
      if (shouldShowCompletedJobs) {
        content ++=
          <div id="completed-jobs">
            <h4 id="completed">Completed Jobs (<span class="completed-job-count">{completedJobs.size}</span>)</h4>
            { completedJobsTable }
          </div>
      }
      if (shouldShowFailedJobs) {
        content ++=
          <div id="failed-jobs">
            <h4 id ="failed">Failed Jobs (<span class="failed-job-count">{failedJobs.size}</span>)</h4>
            { failedJobsTable }
          </div>
      }
      val helpText = """A job is triggered by an action, like "count()" or "saveAsTextFile()".""" +
        " Click on a job's title to see information about the stages of tasks associated with" +
        " the job."

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }
  }
}

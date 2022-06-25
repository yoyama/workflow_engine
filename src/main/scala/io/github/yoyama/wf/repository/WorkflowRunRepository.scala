package io.github.yoyama.wf.repository

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.workflow.WorkflowDagOps
import io.github.yoyama.wf.db.model.running.WorkflowRunAll

import scala.util.Try

trait WorkflowRunRepository() {
  def getWorkflowRun(runId:Int):Transaction[WorkflowRun]
  def getTaskRun(runId:Int):Transaction[Seq[TaskRun]]
  def getLinkRun(runId:Int):Transaction[Seq[LinkRun]]

  def assignNewRunId():Transaction[Int]
  // Save a workflow to all related tables. if wfid is None, assign new id.
  def saveNewWorkflowRunAll(wfa:WorkflowRunAll, runId:Option[Int] = None):Transaction[WorkflowRunAll]

  // Update a workflow to all related tables. Existing records are deleted then inserted.
  def updateWorkflowRunAll(wfa: WorkflowRunAll):Transaction[WorkflowRunAll]
  
  def deleteLinkRun(runId:Int):Transaction[Int]
  def deleteTaskRun(runId:Int):Transaction[Int]
}



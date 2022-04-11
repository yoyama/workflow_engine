package io.github.yoyama.wf.repository

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.workflow.WorkflowDagOps
import io.github.yoyama.wf.db.model.running.WorkflowRunAll

import scala.util.Try

trait WorkflowRepository() {
  def getWorkflowRun(id:Int):Transaction[Option[WorkflowRun]]
  def getTaskRun(wfid:Int):Transaction[Seq[TaskRun]]
  def getLinkRun(wfid:Int):Transaction[Seq[LinkRun]]

  def assignNewWfId():Transaction[Int]
  // Save a workflow to all related tables. if wfid is None, assign new id.
  def saveNewWorkflowRunAll(wfa:WorkflowRunAll, wfid:Option[Int] = None):Transaction[WorkflowRunAll]

  // Update a workflow to all related tables. Existing records are deleted then inserted.
  def updateWorkflowRunAll(wfa: WorkflowRunAll):Transaction[WorkflowRunAll]
}



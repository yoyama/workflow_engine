package io.github.yoyama.wf.repository

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.workflow.WorkflowDagOps

import scala.util.Try

trait WorkflowRepository() {
  def getWorkflowRun(id:Int):Transaction[Option[WorkflowRun]]
  def getTaskRun(wfid:Int):Transaction[Seq[TaskRun]]
  def getLinkRun(wfid:Int):Transaction[Seq[LinkRun]]

  def assignNewWfId():Transaction[Int]
  // Save a workflow to all related tables. if wfid is None, assign new id.
  def saveNewWorkflowRunAll(wf: WorkflowRun, tasks:Seq[TaskRun], links:Seq[LinkRun], wfid:Option[Int] = None):Transaction[(WorkflowRun, Seq[TaskRun], Seq[LinkRun])]

  // Update a workflow to all related tables. Existing records are deleted then inserted.
  def updateWorkflowRunAll(wf: WorkflowRun, tasks:Seq[TaskRun], links:Seq[LinkRun]):Transaction[(WorkflowRun, Seq[TaskRun], Seq[LinkRun])]
}



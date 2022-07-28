package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.messaging.*
import io.github.yoyama.wf.workflow.WorkflowDagOps

import scala.util.Try

class WorkflowProcessor(val dagops: WorkflowDagOps /** , val tQueue:TaskQueue, val wfQueue:WorkflowQueue */) {
  def receiveLoop():Unit = {
    // Fetch message
    // Load WfDag
    // Update DB
    // Send message to TaskDispatcher
  }

  def startWorkflow(dag:WorkflowDag):Try[WorkflowDag] = {
    dagops.startWorkflow(dag)
  }
}

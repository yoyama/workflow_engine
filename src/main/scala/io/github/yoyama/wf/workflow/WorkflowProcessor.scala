package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.messaging.{Queue, TaskQueue, WorkflowQueue}
import io.github.yoyama.wf.workflow.WorkflowDagOps

class WorkflowProcessor(val dagops: WorkflowDagOps, val tQueue:TaskQueue, val wfQueue:WorkflowQueue) {
  type WfDag = dagops.WorkflowDag
  def receiveLoop():Unit = {
    // Fetch message
    // Load WfDag
    // Update DB
    // Send message to TaskDispatcher
  }

  def startWorkflow(dag:WfDag):Unit = {
  }
}

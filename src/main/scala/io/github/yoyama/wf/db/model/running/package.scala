package io.github.yoyama.wf.db.model

package object running {
  case class WorkflowRunAll(wf:WorkflowRun, tasks:Seq[TaskRun], links:Seq[LinkRun])
}

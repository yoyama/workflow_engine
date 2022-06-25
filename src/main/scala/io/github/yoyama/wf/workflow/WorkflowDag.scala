package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.{RunID, TaskID}
import io.github.yoyama.wf.dag.Dag

// A dag for workflow
case class WorkflowDag(id:RunID, dag:Dag, tasks:Map[TaskID,WorkflowTask], tags:Map[String,String]) {
  def getTask(id:TaskID):Option[WorkflowTask] = tasks.get(id)
  def getParents(id:TaskID):Seq[TaskID] = dag.parents.get(id).map(_.toSeq).getOrElse(Seq.empty)
  def getChildren(id:TaskID):Seq[TaskID] = dag.children.get(id).map(_.toSeq).getOrElse(Seq.empty)

  def printInfo: String = {
    val sb = new StringBuilder()
    sb.append(s"Workflow ID: ${id}  ")
    sb.append(s"root: id ${dag.root.id}  ")
    sb.append(s"terminal: id ${dag.terminal.id}\n")
    sb.append(s"tasks: ")
    sb.append(tasks.toSeq.sortBy(x => x._1).map(x => s"${x._2.id} ${x._2.name}").mkString(","))
    sb.toString()
  }
}

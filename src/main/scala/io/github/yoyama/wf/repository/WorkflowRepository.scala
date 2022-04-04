package io.github.yoyama.wf.repository

import io.github.yoyama.wf.db.model.running.{WorkflowRun,TaskRun,LinkRun}
import io.github.yoyama.wf.workflow.WorkflowDagOps
import scala.util.Try

trait WorkflowRepository() {
  def getWorkflowRun(id:Int):Transaction[Option[WorkflowRun]]
  def getTaskRun(wfid:Int):Transaction[Seq[TaskRun]]
  def getLinkRun(wfid:Int):Transaction[Seq[LinkRun]]
}

class DatabaseWorkflowStore extends WorkflowRepository {
  import scalikejdbc._
  import scalikejdbc.config._
  def getWorkflowRun(id:Int):Transaction[Option[WorkflowRun]] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      WorkflowRun.find(id)(session)
    }
  }
  def getTaskRun(wfid:Int):Transaction[Seq[TaskRun]] = ???
  def getLinkRun(wfid:Int):Transaction[Seq[LinkRun]] = ???

}

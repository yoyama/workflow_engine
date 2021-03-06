package io.github.yoyama.wf.repository

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.db.model.running.WorkflowRunAll
import cats.*
import cats.implicits.*
import io.github.yoyama.wf.db.model.running
import scalikejdbc.*
import scalikejdbc.config.*

class DatabaseWorkflowRunRepository extends WorkflowRunRepository {
  def getWorkflowRunAll(runId: Int): Transaction[WorkflowRunAll] = {
    for {
      w <- getWorkflowRun(runId)
      tasks <- getTaskRun(runId)
      links <- getLinkRun(runId)
    } yield WorkflowRunAll(w, tasks, links)
  }

  def getWorkflowRun(runId: Int): Transaction[WorkflowRun] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      WorkflowRun.find(runId)(session) match {
        case Some(w) => w
        case None => throw new NoSuchElementException(runId.toString)
      }
    }
  }

  def getTaskRun(runId: Int): Transaction[Seq[TaskRun]] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      TaskRun.findAllBy(sqls"run_id = ${runId}")(session)
    }
  }

  def getLinkRun(runId: Int): Transaction[Seq[LinkRun]] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      LinkRun.findAllBy(sqls"run_id = ${runId}")(session)
    }
  }

  def assignNewRunId(): Transaction[Int] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      implicit val s = session
      sql"""select nextval('running.run_id')""".map(rs => rs.int("nextval")).single.apply().get
    }
  }

  // Create a workflow to all related tables. if runId is None, assign new ID.
  def saveNewWorkflowRunAll(wfa: WorkflowRunAll, runId: Option[Int] = None): Transaction[WorkflowRunAll] = {
    for {
      runId2 <- runId.map(i => ScalikeJDBCTransaction.from(i)).getOrElse(assignNewRunId())
      wf2: WorkflowRun <- ScalikeJDBCTransaction.from(ss => {
        WorkflowRun.create(wfa.wf.copy(runId = runId2))(ss)
      })
      tasks2:List[TaskRun] <- wfa.tasks
        .map(t => ScalikeJDBCTransaction.from(ss =>  TaskRun.create(t.copy(runId = runId2))(ss)))
        .toList.traverse(identity)
      links2:List[LinkRun] <- wfa.links
        .map(l => ScalikeJDBCTransaction.from(ss => LinkRun.create(l.copy(runId = runId2))(ss)))
        .toList.traverse(identity)
    } yield WorkflowRunAll(wf2, tasks2, links2)
  }

  // Update a workflow to all related tables. Existing records are deleted then inserted.
  def updateWorkflowRunAll(wfa: WorkflowRunAll): Transaction[WorkflowRunAll] = {
    for {
      wf2 <- ScalikeJDBCTransaction.from(ss => {
        WorkflowRun.save(wfa.wf)(ss)
      })
      delLinks <- deleteLinkRun(wf2.runId)
      delTasks <- deleteTaskRun(wf2.runId)
      tasks2 <- wfa.tasks
        .map(t => ScalikeJDBCTransaction.from(ss =>  TaskRun.create(t)(ss)))
        .toList.traverse(identity)
      links2 <- wfa.links
        .map(l => ScalikeJDBCTransaction.from(ss => LinkRun.create(l)(ss)))
        .toList.traverse(identity)
    } yield WorkflowRunAll(wf2, tasks2, links2)
  }

  def updateTaskRun(taskRun: TaskRun): Transaction[TaskRun] = {
    ScalikeJDBCTransaction.from(ss => {
      TaskRun.update(taskRun)(ss)
      TaskRun.find(taskRun.taskId, taskRun.runId)(ss).get
    })
  }

  def deleteLinkRun(runId:Int):Transaction[Int] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      implicit val s = session
      sql"""delete from running.link_run where run_id = ${runId}""".update.apply()
    }
  }

  def deleteTaskRun(runId:Int):Transaction[Int] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      implicit val s = session
      sql"""delete from running.task_run where run_id = ${runId}""".update.apply()
    }
  }

  def updateWorkflowRunState(runId:Int, state:Int):Transaction[WorkflowRun] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      implicit val s = session
      WorkflowRun.updateState(runId, state)
      WorkflowRun.find(runId).get
    }
  }

  def updateTaskRunState(runId:Int, taskId:Int, state:Int):Transaction[TaskRun] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      implicit val s = session
      TaskRun.updateState(runId = runId, taskId = taskId, state)
      TaskRun.find(taskId = taskId, runId = runId).get
    }
  }

}

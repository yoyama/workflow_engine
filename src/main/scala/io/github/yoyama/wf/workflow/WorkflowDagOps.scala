package io.github.yoyama.wf.workflow

import io.github.yoyama.utils.OptionHelper._
import io.github.yoyama.wf.WfID
import io.github.yoyama.wf.dag.{CellID,Link,Id2Cell,Dag,DagOps,DagCell}
import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.repository.{Transaction, TransactionResult, TransactionRunner, WorkflowRepository}

import java.time.Instant
import scala.util.{Success, Try}

case class TaskNotFoundException(id:CellID) extends RuntimeException

// A task of Dag for workflow
// state: 0:wait 1:ready 5:provisioning 11:initializing 21:running 31:post processing 98:stopping 99:stop
case class WorkflowTask(id:CellID, name: String, tType: String, config: String, state: Int = 0,
                        result: Option[Int] = None, errorCode: Option[Int] = None,
                        startAt: Option[Instant] = None, finishAt: Option[Instant] = None, tags:Map[String,String] = Map.empty)
// A dag for workflow
case class WorkflowDag(id:WfID, dag:Dag, tasks:Map[CellID,WorkflowTask], tags:Map[String,String]) {
  def getTask(id:CellID):Option[WorkflowTask] = tasks.get(id)
  def getParents(id:CellID):Seq[CellID] = dag.parents.get(id).map(_.toSeq).getOrElse(Seq.empty)
  def getChildren(id:CellID):Seq[CellID] = dag.children.get(id).map(_.toSeq).getOrElse(Seq.empty)

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


class WorkflowDagOps(val wfRepo:WorkflowRepository)(implicit val tRunner:TransactionRunner) extends DagOps {

  // Create WorkflowDag by loading data in DB
  def loadWorkflow(wfid:WfID): Try[WorkflowDag] = {
    val transaction: Transaction[(WorkflowRun, Seq[TaskRun], Seq[LinkRun])] = for {
      wf <- wfRepo.getWorkflowRun(wfid)
      t <- wfRepo.getTaskRun(wfid)
      l <- wfRepo.getLinkRun(wfid)
    } yield (wf,t, l)
    for {
      tResult <-transaction.run.v.toTry
      wfRun:WorkflowRun <- Success(tResult._1)
      wf <- createWorkflow(wfRun, tResult._2, tResult._3)
    } yield wf
  }

  def createWorkflow(wfR:WorkflowRun, tasksR:Seq[TaskRun], linksR:Seq[LinkRun]): Try[WorkflowDag] = {
    import cats.implicits._
    def convTask(tr:TaskRun):Try[WorkflowTask] = {
      Success(WorkflowTask(
        id = tr.id,
        name = tr.name,
        tType = tr.`type`,
        config = tr.config.toString,
        state = tr.state,
        result = tr.result,
        errorCode = tr.errCode,
        startAt = tr.startAt.map(_.toInstant),
        finishAt = tr.finishAt.map(_.toInstant),
        //ToDo tags
      ))
    }
    def convLink(lr:Seq[LinkRun]):Try[Seq[(CellID,CellID)]] = {
      val ret = lr.map(l => (l.parent, l.child))
      Success(ret)
    }

    for {
      tasks: Seq[WorkflowTask] <- tasksR.toList.traverse(convTask)
      links: Seq[(CellID,CellID)] <- convLink(linksR)
      wf <- createWorkflow(wfR.runId, tasks, links)
    } yield wf
  }

  def createWorkflow(id:WfID, wfTasks:Seq[WorkflowTask], pairs:Seq[(CellID,CellID)], tags:Map[String,String] = Map.empty):Try[WorkflowDag] = {
    def convLink(pairs:Seq[(CellID,CellID)]):Try[(Link,Link)] = {
      val ret = pairs.foldLeft((Map.empty[Int,Set[Int]], Map.empty[Int,Set[Int]])) { (acc, c) =>
        val (pLink:Link, cLink:Link) = acc
        val (pid:CellID, cid:CellID) = c
        val newPLink: Link = pLink.get(cid) match {
          case Some(values) => pLink.updated(cid, values + pid )
          case None => pLink.updated(cid, Set(pid))
        }
        val newCLink: Link = cLink.get(pid) match {
          case Some(values) => cLink.updated(pid, values + cid )
          case None => cLink.updated(pid, Set(cid))
        }
        (newPLink, newCLink)
      }
      Success(ret)
    }
    def convCells(tasks:Seq[WorkflowTask]):Try[Map[CellID,DagCell]] = {
      Success(
        tasks
          .map(t => (t.id, DagCell(t.id, t.state, Instant.now())))
          .toMap
      )
    }
    for {
      cells <- convCells(wfTasks)
      (plinks,clinks) <- convLink(pairs)
      root <- cells.get(0).toTry("No root cell")
      terminal <- cells.get(-1).toTry("No terminal cell")
      dag <- Success(Dag(root, terminal, cells, plinks, clinks))
      id2wftasks <- Success(wfTasks.map(t => (t.id, t)).toMap)
    } yield WorkflowDag(id, dag, id2wftasks, tags)
  }

  def fetchNextTasks(wfDag:WorkflowDag, id:CellID):Try[Seq[WorkflowTask]] = {
    import cats.implicits._

    val children = wfDag.getChildren(id)
    val readyChildren = children.filter( c => {
      val parents = wfDag.getParents(c).filter(p => wfDag.getTask(p).get.state != 99)
      parents.size == 0 // check, the child of all parents are in stop state)
    })
    val tasks: Seq[Try[WorkflowTask]] = readyChildren.map(id => wfDag.getTask(id).toTry(TaskNotFoundException(id)))
    tasks.sequence // convert Seq[Try[_]} to Try[Seq[_]] by cats
  }

  def updateTaskStates(wfDag:WorkflowDag, ids:Seq[CellID], state:Int): Try[WorkflowDag] = {

    ???
  }

  def runNextTasks(wfDag:WorkflowDag): Try[(WorkflowDag, Seq[WorkflowTask])] = ???

  def saveWorkflow(wfDag:WorkflowDag):Try[WorkflowDag] = ???
}


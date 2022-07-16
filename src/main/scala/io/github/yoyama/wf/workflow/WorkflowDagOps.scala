package io.github.yoyama.wf.workflow

import io.github.yoyama.utils.OptionHelper.*
import io.github.yoyama.wf.dag.*
import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun, WorkflowRunAll}
import io.github.yoyama.wf.repository.{ScalikeJDBCTransaction, Transaction, TransactionResult, TransactionRunner, WorkflowRunRepository}
import io.github.yoyama.wf.{RunID, TaskID, repository}

import java.time.Instant
import scala.util.control.Exception.*
import scala.util.{Success, Try}
import cats.implicits.*
import io.github.yoyama.wf.tag.Tag

case class TaskNotFoundException(id:TaskID) extends RuntimeException

class WorkflowDagOps(val wfRepo:WorkflowRunRepository)(implicit val tRunner:TransactionRunner) extends DagOps {

  // Create WorkflowDag by loading data in DB
  def loadWorkflowDag(wfid: RunID): Try[WorkflowDag] = {
    val transaction: Transaction[(WorkflowRun, Seq[TaskRun], Seq[LinkRun])] = for {
      wf <- wfRepo.getWorkflowRun(wfid)
      t <- wfRepo.getTaskRun(wfid)
      l <- wfRepo.getLinkRun(wfid)
    } yield (wf, t, l)
    for {
      tResult <- transaction.run.v.toTry
      wfRun: WorkflowRun <- Success(tResult._1)
      wf <- buildWorkflowDag(wfRun, tResult._2, tResult._3)
    } yield wf
  }

  def toWorkflowTask(tr:TaskRun): Try[WorkflowTask] = {
    for {
      tags <- Tag.from(tr.tags)
    } yield WorkflowTask(
      id = tr.taskId,
      runId = tr.runId,
      name = tr.name,
      tType = tr.`type`,
      config = tr.config,
      state = TaskState(tr.state),
      result = tr.result,
      errorCode = tr.errCode,
      startAt = tr.startAt,
      finishAt = tr.finishAt,
      createdAt = tr.createdAt,
      updatedAt = tr.updatedAt,
      tags = tags
    )
  }

  // Build WorkflowDag from model data
  def buildWorkflowDag(wfR: WorkflowRun, tasksR: Seq[TaskRun], linksR: Seq[LinkRun]): Try[WorkflowDag] = {

    def convLink(lr: Seq[LinkRun]): Try[Seq[(TaskID, TaskID)]] = {
      val ret = lr.map(l => (l.parent, l.child))
      Success(ret)
    }

    for {
      tasks: Seq[WorkflowTask] <- tasksR.toList.traverse(toWorkflowTask)
      links: Seq[(TaskID, TaskID)] <- convLink(linksR)
      wfTag: Tag <- Tag.from(wfR.tags)
      wf <- buildWorkflowDag(wfR.runId, wfR.name, tasks, links, tags = wfTag)
    } yield wf
  }

  // Build WorkflowDag from WorkflowTask and task link (from, to)
  def buildWorkflowDag(id: RunID, name: String, wfTasks: Seq[WorkflowTask], pairs: Seq[(TaskID, TaskID)], tags: Tag): Try[WorkflowDag] = {
    def convLink(pairs: Seq[(TaskID, TaskID)]): Try[(LinkMap, LinkMap)] = {
      val ret = pairs.foldLeft((Map.empty[Int, Set[Int]], Map.empty[Int, Set[Int]])) { (acc, c) =>
        val (pLink: LinkMap, cLink: LinkMap) = acc
        val (pid: TaskID, cid: TaskID) = c
        val newPLink: LinkMap = pLink.get(cid) match {
          case Some(values) => pLink.updated(cid, values + pid)
          case None => pLink.updated(cid, Set(pid))
        }
        val newCLink: LinkMap = cLink.get(pid) match {
          case Some(values) => cLink.updated(pid, values + cid)
          case None => cLink.updated(pid, Set(cid))
        }
        (newPLink, newCLink)
      }
      Success(ret)
    }

    def convCells(tasks: Seq[WorkflowTask]): Try[Map[TaskID, DagCell]] = {
      Success(
        tasks
          .map(t => (t.id, DagCell(t.id, t.state.value, Instant.now())))
          .toMap
      )
    }

    for {
      cells <- convCells(wfTasks)
      (plinks, clinks) <- convLink(pairs)
      root <- cells.get(0).toTry("No root cell")
      terminal <- cells.get(-1).toTry("No terminal cell")
      dag <- Success(Dag(root, terminal, cells, plinks, clinks))
      id2wftasks <- Success(wfTasks.map(t => (t.id, t)).toMap)
    } yield WorkflowDag(id, name, dag, id2wftasks, tags = tags)
  }

  def fetchNextTasks(wfDag: WorkflowDag, id: TaskID): Try[Seq[WorkflowTask]] = {
    val children = wfDag.getChildren(id)
    val readyChildren = children.filter(c => {
      val parents = wfDag.getParents(c).filter(p => wfDag.getTask(p).get.state != TaskState.STOP)
      parents.size == 0 // check, the child of all parents are in stop state)
    })
    val tasks: Seq[Try[WorkflowTask]] = readyChildren.map(id => wfDag.getTask(id).toTry(TaskNotFoundException(id)))
    tasks.sequence // convert Seq[Try[_]} to Try[Seq[_]] by cats
  }

  def updateTasksState(wfDag: WorkflowDag, ids: Seq[TaskID], state: TaskState): Try[WorkflowDag] = {
    val tasks: Seq[Transaction[TaskRun]] = ids.map(id => wfRepo.updateTaskRunState(wfDag.id, id, state.value))
    val t: Transaction[List[TaskRun]] = tasks.map(_.asInstanceOf[ScalikeJDBCTransaction[TaskRun]]).toList.traverse(identity)
    for {
      taskRuns <- t.run.v.toTry
      wfTasks <- taskRuns.traverse(toWorkflowTask)
      newDag <- replaceWorkflowTasks(wfDag, wfTasks)
    } yield newDag
  }

  private def replaceWorkflowTasks(wfDag: WorkflowDag, newTasks:Seq[WorkflowTask]): Try[WorkflowDag] = {
    for {
      _   <- newTasks.map(_.id).traverse(wfDag.getTask).toTry(s"""Invalid tasks: ${newTasks}""")
      updated <- catching(classOf[RuntimeException]) withTry {
        newTasks.foldLeft(wfDag) { (dag, task) =>
          dag.copy(tasks = dag.tasks.updated(task.id, task))
        }
      }
    } yield updated
  }

  def updateWorkflowState(wfDag: WorkflowDag, state: WorkflowState): Try[WorkflowDag] = {
    ???
  }

  def startWorkflow(wfDag:WorkflowDag): Try[WorkflowDag] = {
    updateWorkflowState(wfDag, WorkflowState.READY)
  }

  def runNextTasks(wfDag: WorkflowDag): Try[(WorkflowDag, Seq[WorkflowTask])] = {
    ???
  }

  def saveNewWorkflowDag(wfDag: WorkflowDag): Try[WorkflowDag] = {
    for {
      runAll <- toWorkflowRunAll(wfDag)
      newRunAll <- wfRepo.saveNewWorkflowRunAll(runAll, None).run.v.toTry
      newWfDag <- loadWorkflowDag(newRunAll.wf.runId)
    } yield newWfDag
  }

  def toWorkflowRunAll(wfDag: WorkflowDag): Try[WorkflowRunAll] = {
    import cats.implicits._
    for {
      wf <- toWorkflowRun(wfDag)
      tasks <- wfDag.tasks.values.toList.map(toTaskRun(_)).sequence
      links <- toLinkRun(wfDag)
    } yield WorkflowRunAll(
      wf = wf,
      tasks = tasks,
      links = links
    )
  }

  def toWorkflowRun(wfDag: WorkflowDag): Try[WorkflowRun] = {
    val ret = catching(classOf[RuntimeException]) either {
      WorkflowRun(
        runId = wfDag.id,
        name = wfDag.name,
        state = wfDag.state.value,
        startAt = wfDag.startAt,
        finishAt = wfDag.finishAt,
        tags = Some(wfDag.tags.toString()), // ToDo
        createdAt = wfDag.createdAt.getOrElse(null),
        updatedAt = wfDag.updatedAt.getOrElse(null)
      )
    }
    ret.toTry
  }

  def toTaskRun(wfTask: WorkflowTask): Try[TaskRun] = {
    val ret: Either[Throwable, TaskRun] = catching(classOf[RuntimeException]) either {
      TaskRun(
        taskId = wfTask.id,
        runId = wfTask.runId,
        name = wfTask.name,
        `type` = wfTask.tType,
        config = wfTask.config,
        state = wfTask.state.value,
        result = wfTask.result,
        errCode = wfTask.errorCode,
        startAt = wfTask.startAt,
        finishAt = wfTask.finishAt,
        tags = Some(wfTask.tags.toString()), // ToDo
        createdAt = wfTask.createdAt,
        updatedAt = wfTask.updatedAt
      )
    }
    ret.toTry
  }

  def toLinkRun(wfDag: WorkflowDag): Try[Seq[LinkRun]] = {
    val ret = catching(classOf[RuntimeException]) either {
      val p2c: Seq[(Int, Int)] = wfDag.dag.children.toList.flatMap(x => x._2.map((x._1, _)))
      p2c.map(x => LinkRun(runId = wfDag.id, parent = x._1, child = x._2, createdAt = Instant.now()))
    }
    ret.toTry
  }
}


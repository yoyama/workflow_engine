package io.github.yoyama.wf.workflow

import io.github.yoyama.utils.OptionHelper.*
import io.github.yoyama.wf.dag.*
import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun, WorkflowRunAll}
import io.github.yoyama.wf.repository.{ScalikeJDBCTransaction, Transaction, TransactionResult, TransactionRunner, WorkflowRunRepository}
import io.github.yoyama.wf.{RunID, TaskID, repository}

import java.time.Instant
import scala.util.control.Exception.*
import scala.util.{Failure, Success, Try}
import cats.implicits.*
import io.github.yoyama.wf.tag.Tag

case class TaskNotFoundException(id:TaskID) extends RuntimeException

class WorkflowDagOps(val wfRepo:WorkflowRunRepository)(implicit val tRunner:TransactionRunner) { //extends DagOps {
  val wfBuilder = new WorkflowDagBuilder(wfRepo)(tRunner)

  protected def fetchNextTasks(wfDag: WorkflowDag, id: TaskID): Try[Seq[WorkflowTask]] = {
    val children = wfDag.getChildren(id)
    val readyChildren = children.filter(c => {
      val parents = wfDag.getParents(c).filter(p => wfDag.getTask(p).get.state != TaskState.STOP)
      parents.size == 0 // check, the child of all parents are in stop state)
    })
    val tasks: Seq[Try[WorkflowTask]] = readyChildren.map(id => wfDag.getTask(id).toTry(TaskNotFoundException(id)))
    tasks.sequence // convert Seq[Try[_]} to Try[Seq[_]] by cats
  }

  protected def finishSuccessfully(wfDag: WorkflowDag, id:TaskID): Boolean = {
    val task = wfDag.getTask(id).get
    task.state == TaskState.STOP && task.result == TaskResult.SUCCESS
  }

  protected def beReady(wfDag: WorkflowDag, id:TaskID): Try[Boolean] = {
    for {
      task: WorkflowTask <- wfDag.getTask(id).toTry(s"No task: ${id}")
      _ <- if (task.state == TaskState.WAIT) Success(task) else Failure(InvalidTaskState(s"""The task ${id} is not in state WAIT"""))
      parents <- Success(wfDag.getParents(id))
      notStopParents <- catching(classOf[RuntimeException]) withTry parents.filter(p => !finishSuccessfully(wfDag, p)
      )
    } yield notStopParents.size == 0
  }

  def updateTasksState(wfDag: WorkflowDag, ids: Seq[TaskID], state: TaskState): Try[WorkflowDag] = {
    val tasks: Seq[Transaction[TaskRun]] = ids.map(id => wfRepo.updateTaskRunState(wfDag.id, id, state.value))
    val t: Transaction[List[TaskRun]] = tasks.map(_.asInstanceOf[ScalikeJDBCTransaction[TaskRun]]).toList.traverse(identity)
    for {
      taskRuns <- t.run.v.toTry
      wfTasks <- taskRuns.traverse(wfBuilder.toWorkflowTask)
      newDag <- replaceWorkflowTasks(wfDag, wfTasks)
    } yield newDag
  }

  protected def replaceWorkflowTasks(wfDag: WorkflowDag, newTasks:Seq[WorkflowTask]): Try[WorkflowDag] = {
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

  def runNextTasks(wfDag: WorkflowDag, id:TaskID): Try[(WorkflowDag, Seq[WorkflowTask])] = {
    for {
      tasks <- fetchNextTasks(wfDag, id)
      updatedDag <- updateTasksState(wfDag, tasks.map(_.id), TaskState.READY)
      updatedTasks <- tasks.map(t => updatedDag.getTask(t.id).toTry(TaskNotFoundException(t.id))).toList.sequence
    } yield (updatedDag, updatedTasks)
  }

  /**
   * Submit new Workflow
   * @param wfDag
   * @return
   */
  def submitWorkflowDag(wfDag: WorkflowDag): Try[WorkflowDag] = {
    for {
      //ToDo validate wfDag: state, dag validation
      runAll <- toWorkflowRunAll(wfDag)
      newRunAll <- wfRepo.saveNewWorkflowRunAll(runAll, None).run.v.toTry
      newWfDag <- wfBuilder.loadWorkflowDag(newRunAll.wf.runId)
    } yield newWfDag
  }

  private def toWorkflowRunAll(wfDag: WorkflowDag): Try[WorkflowRunAll] = {
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

  private def toWorkflowRun(wfDag: WorkflowDag): Try[WorkflowRun] = {
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

  private def toTaskRun(wfTask: WorkflowTask): Try[TaskRun] = {
    val ret: Either[Throwable, TaskRun] = catching(classOf[RuntimeException]) either {
      TaskRun(
        taskId = wfTask.id,
        runId = wfTask.runId,
        name = wfTask.name,
        `type` = wfTask.tType,
        config = wfTask.config,
        state = wfTask.state.value,
        result = TaskResult.toValue(wfTask.result),
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

  private def toLinkRun(wfDag: WorkflowDag): Try[Seq[LinkRun]] = {
    val ret = catching(classOf[RuntimeException]) either {
      val p2c: Seq[(Int, Int)] = wfDag.dag.children.toList.flatMap(x => x._2.map((x._1, _)))
      p2c.map(x => LinkRun(runId = wfDag.id, parent = x._1, child = x._2, createdAt = Instant.now()))
    }
    ret.toTry
  }
}


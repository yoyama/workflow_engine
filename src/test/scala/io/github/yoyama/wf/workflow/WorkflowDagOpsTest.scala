package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun, WorkflowRunAll}
import io.github.yoyama.wf.repository.{DatabaseWorkflowRunRepository, ScalikeJDBCTransactionRunner, Transaction, TransactionResult, TransactionRunner, WorkflowRunRepository}
import io.github.yoyama.wf.tag.Tag
import org.scalatest.flatspec.AnyFlatSpec
import scalikejdbc.ConnectionPool

import scala.util.{Failure, Success, Try}

class WorkflowDagOpsTest extends AnyFlatSpec {
  val jdbcUrl = sys.env.getOrElse("TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/test_workflow")
  val jdbcUser = sys.env.getOrElse("TEST_JDBC_USER", "test")
  val jdbcPass = sys.env.getOrElse("TEST_JDBC_PASS", "testtest")
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPass)

  implicit val tRunner:TransactionRunner = new ScalikeJDBCTransactionRunner()
  val wfRepo = new DatabaseWorkflowRunRepository
  val wfops = new WorkflowDagOps(wfRepo)

  val testTasks = Seq(
    WorkflowTask(0, 99, "root", "nop", "{}", state = TaskState.READY, createdAt = null, updatedAt = null),
    WorkflowTask(-1, 99, "terminal", "nop", "{}", state = TaskState.WAIT, createdAt = null, updatedAt = null),
    WorkflowTask(1, 99, "task1", "nop", "{}", state = TaskState.WAIT, createdAt = null, updatedAt = null),
    WorkflowTask(2, 99, "task2", "nop", "{}", state = TaskState.WAIT, createdAt = null, updatedAt = null),
    WorkflowTask(3, 99, "task3", "nop", "{}", state = TaskState.WAIT, createdAt = null, updatedAt = null),
  )
  val testLinks = Seq((0, 1), (0, 2), (1, 3), (2, 3), (3, -1))
  val testTag = Tag.from("""{ "type" : "normal" } """).get

  "submitWorkflowDag" should "work" in {
    val ret = for {
      wfDag1 <- wfops.wfBuilder.buildWorkflowDag(99, "wf1", testTasks, testLinks, tags = testTag)
      dag <- wfops.submitWorkflowDag(wfDag1)
    } yield dag
    ret match {
      case Failure(e) => fail(e)
      case Success(dag) =>
        println(dag)
        assert("normal" == dag.tags.getString("type").get)
        assert(dag.getChildren(0).toSet == Set(1,2))
        assert(dag.getChildren(1).toSet == Set(3))
        assert(dag.getChildren(2).toSet == Set(3))
        assert(dag.getChildren(3).toSet == Set(-1))
    }
  }
  
  "updateTasksState" should "work" in {
    val ret = for {
      wfDag1 <- wfops.wfBuilder.buildWorkflowDag(99, "wf1", testTasks, testLinks, tags = testTag)
      dag1 <- wfops.submitWorkflowDag(wfDag1)
      dag2 <- wfops.updateTaskState(dag1, Seq(0), TaskState.STOP)
      dag3 <- wfops.updateTaskState(dag2, Seq(1, 2), TaskState.RUNNING)
    } yield dag3
    ret match {
      case Failure(e) => fail(e)
      case Success(dag) =>
        println(dag)
        assert(dag.getTask(0).get.state == TaskState.STOP)
        assert(dag.getTask(1).get.state == TaskState.RUNNING)
        assert(dag.getTask(2).get.state == TaskState.RUNNING)
        assert(dag.getTask(3).get.state == TaskState.WAIT)
        assert(dag.getTask(-1).get.state == TaskState.WAIT)
    }
  }

  "runNextTasks" should "work" in {
    val ret = for {
      wfDag1 <- wfops.wfBuilder.buildWorkflowDag(99, "wf1", testTasks, testLinks, tags = testTag)
      dag1 <- wfops.submitWorkflowDag(wfDag1)
      (dag2, tasks2) <- wfops.runNextTasks(dag1, 0)
      dag3 <- wfops.updateTaskState(dag2, Seq(0), TaskState.STOP)
      (dag4, tasks4) <- wfops.runNextTasks(dag3, 0)
      dag5 <- wfops.updateTaskState(dag2, Seq(1,2), TaskState.STOP)
      (dag6, tasks6) <- wfops.runNextTasks(dag5, 1)
      (dag7, tasks7) <- wfops.runNextTasks(dag6, 2)
      _ <- {
        println("before finish task 0:" + tasks2)
        assert(tasks2.size == 0)
        Success(true)
      }
      _ <- {
        println("after finish task 0:" + tasks4)
        assert(tasks4.size == 2)
        Success(true)
      }
      _ <- {
        println("after finish task1,2: " + tasks6)
        assert(tasks6.size == 1)
        Success(true)
      }
      _ <- {
        println("after finish task1,2 and already updated: " + tasks7)
        assert(tasks7.size == 0)
        Success(true)
      }
    } yield dag7
    println(ret)
  }
}


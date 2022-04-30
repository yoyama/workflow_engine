package io.github.yoyama.wf.repository
import io.github.yoyama.wf.db.model.running.{WorkflowRunAll, WorkflowRun, TaskRun}
import scalikejdbc.*
import scalikejdbc.config.*
import org.scalatest.flatspec.AnyFlatSpec

import java.time.ZonedDateTime

class DatabaseWorkflowRepositoryTest  extends AnyFlatSpec {
  Class.forName("org.postgresql.Driver")
  //DBs.setup()
  val jdbcUrl = sys.env.getOrElse("TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/test_workflow")
  val jdbcUser = sys.env.getOrElse("TEST_JDBC_USER", "test")
  val jdbcPass = sys.env.getOrElse("TEST_JDBC_PASS", "testtest")
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPass)
  implicit val transactionRunner:TransactionRunner = new ScalikeJDBCTransactionRunner()

  "assignNewWfId" should "work" in {
    val repo = new DatabaseWorkflowRepository()
    val ret = repo.assignNewWfId().run
    val id1 = {
      val ret = repo.assignNewWfId().run
      assert(ret.v.isRight)
      ret.v.getOrElse(-1)
    }
    val id2 = {
      val ret = repo.assignNewWfId().run
      assert(ret.v.isRight)
      ret.v.getOrElse(-1)
    }
    println(id2)
    assert(id2-id1 >= 1)
  }

  "saveNewWorkflowRunAll" should "work" in {
    val now = ZonedDateTime.now()
    val repo = new DatabaseWorkflowRepository()

    val wfa: WorkflowRunAll = WorkflowRunAll(
      wf = WorkflowRun(id = 1, name = "test1", state = 0, createdAt = now, updatedAt = now),
      tasks = Seq(
        TaskRun(id = 2, wfid = 1, name = "t1", `type` ="aaaa", config = "{}", state = 0, createdAt = now, updatedAt = now)
      ),
      links = Seq()
    )
    val ret = repo.saveNewWorkflowRunAll(wfa).run
    println(ret)
  }
}

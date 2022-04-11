package io.github.yoyama.wf.repository
import scalikejdbc._
import scalikejdbc.config._
import org.scalatest.flatspec.AnyFlatSpec

class DatabaseWorkflowRepositoryTest  extends AnyFlatSpec {
  Class.forName("org.postgresql.Driver")
  //DBs.setup()
  val jdbcUrl = sys.env.getOrElse("TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/test_workflow")
  val jdbcUser = sys.env.getOrElse("TEST_JDBC_USER", "test")
  val jdbcPass = sys.env.getOrElse("TEST_JDBC_PASS", "testtest")
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPass)
  implicit val tnasactionRunner:TransactionRunner = new ScalikeJDBCTransactionRunner()

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
    assert(id2-id1 >= 1)
  }
}

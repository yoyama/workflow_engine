package io.github.yoyama.wf.db.model.running

import scalikejdbc._
import java.time.Instant

case class LinkRun(
  runId: Int,
  parent: Int,
  child: Int,
  createdAt: Instant) {

  def save()(implicit session: DBSession): LinkRun = LinkRun.save(this)(session)

  def destroy()(implicit session: DBSession): Int = LinkRun.destroy(this)(session)

}


object LinkRun extends SQLSyntaxSupport[LinkRun] {

  override val schemaName = Some("running")

  override val tableName = "link_run"

  override val columns = Seq("run_id", "parent", "child", "created_at")

  def apply(lr: SyntaxProvider[LinkRun])(rs: WrappedResultSet): LinkRun = apply(lr.resultName)(rs)
  def apply(lr: ResultName[LinkRun])(rs: WrappedResultSet): LinkRun = new LinkRun(
    runId = rs.get(lr.runId),
    parent = rs.get(lr.parent),
    child = rs.get(lr.child),
    createdAt = rs.get(lr.createdAt)
  )

  val lr = LinkRun.syntax("lr")

  override val autoSession = AutoSession

  def find(runId: Int, parent: Int, child: Int)(implicit session: DBSession): Option[LinkRun] = {
    sql"""select ${lr.result.*} from ${LinkRun as lr} where ${lr.runId} = ${runId} and ${lr.parent} = ${parent} and ${lr.child} = ${child}"""
      .map(LinkRun(lr.resultName)).single.apply()
  }

  def findAll()(implicit session: DBSession): List[LinkRun] = {
    sql"""select ${lr.result.*} from ${LinkRun as lr}""".map(LinkRun(lr.resultName)).list.apply()
  }

  def countAll()(implicit session: DBSession): Long = {
    sql"""select count(1) from ${LinkRun.table}""".map(rs => rs.long(1)).single.apply().get
  }

  def findBy(where: SQLSyntax)(implicit session: DBSession): Option[LinkRun] = {
    sql"""select ${lr.result.*} from ${LinkRun as lr} where ${where}"""
      .map(LinkRun(lr.resultName)).single.apply()
  }

  def findAllBy(where: SQLSyntax)(implicit session: DBSession): List[LinkRun] = {
    sql"""select ${lr.result.*} from ${LinkRun as lr} where ${where}"""
      .map(LinkRun(lr.resultName)).list.apply()
  }

  def countBy(where: SQLSyntax)(implicit session: DBSession): Long = {
    sql"""select count(1) from ${LinkRun as lr} where ${where}"""
      .map(_.long(1)).single.apply().get
  }

  def create(l:LinkRun)(implicit session: DBSession): LinkRun = {
    create(l.runId, l.parent, l.child, l.createdAt)(session)
  }

  def create(
    runId: Int,
    parent: Int,
    child: Int,
    createdAt: Instant)(implicit session: DBSession): LinkRun = {
    sql"""
      insert into ${LinkRun.table} (
        ${column.runId},
        ${column.parent},
        ${column.child},
        ${column.createdAt}
      ) values (
        ${runId},
        ${parent},
        ${child},
        ${createdAt}
      )
      """.update.apply()

    LinkRun(
      runId = runId,
      parent = parent,
      child = child,
      createdAt = createdAt)
  }

  def batchInsert(entities: collection.Seq[LinkRun])(implicit session: DBSession): List[Int] = {
    val params: collection.Seq[Seq[(String, Any)]] = entities.map(entity =>
      Seq(
        "runId" -> entity.runId,
        "parent" -> entity.parent,
        "child" -> entity.child,
        "createdAt" -> entity.createdAt))
    SQL("""insert into link_run(
      run_id,
      parent,
      child,
      created_at
    ) values (
      {runId},
      {parent},
      {child},
      {createdAt}
    )""").batchByName(params.toSeq: _*).apply[List]()
  }

  def save(entity: LinkRun)(implicit session: DBSession): LinkRun = {
    sql"""
      update
        ${LinkRun.table}
      set
        ${column.runId} = ${entity.runId},
        ${column.parent} = ${entity.parent},
        ${column.child} = ${entity.child},
        ${column.createdAt} = ${entity.createdAt}
      where
        ${column.runId} = ${entity.runId} and ${column.parent} = ${entity.parent} and ${column.child} = ${entity.child}
      """.update.apply()
    entity
  }

  def destroy(entity: LinkRun)(implicit session: DBSession): Int = {
    sql"""delete from ${LinkRun.table} where ${column.runId} = ${entity.runId} and ${column.parent} = ${entity.parent} and ${column.child} = ${entity.child}""".update.apply()
  }

}

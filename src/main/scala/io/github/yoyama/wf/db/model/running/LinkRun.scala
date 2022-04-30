package io.github.yoyama.wf.db.model.running

import scalikejdbc._
import java.time.{ZonedDateTime}

case class LinkRun(
  id: Int,
  wfid: Int,
  parent: Int,
  child: Int,
  createdAt: ZonedDateTime) {

  def save()(implicit session: DBSession): LinkRun = LinkRun.save(this)(session)

  def destroy()(implicit session: DBSession): Int = LinkRun.destroy(this)(session)

}


object LinkRun extends SQLSyntaxSupport[LinkRun] {

  override val schemaName = Some("running")

  override val tableName = "link_run"

  override val columns = Seq("id", "wfid", "parent", "child", "created_at")

  def apply(lr: SyntaxProvider[LinkRun])(rs: WrappedResultSet): LinkRun = apply(lr.resultName)(rs)
  def apply(lr: ResultName[LinkRun])(rs: WrappedResultSet): LinkRun = new LinkRun(
    id = rs.get(lr.id),
    wfid = rs.get(lr.wfid),
    parent = rs.get(lr.parent),
    child = rs.get(lr.child),
    createdAt = rs.get(lr.createdAt)
  )

  val lr = LinkRun.syntax("lr")

  override val autoSession = AutoSession

  def find(id: Int)(implicit session: DBSession): Option[LinkRun] = {
    sql"""select ${lr.result.*} from ${LinkRun as lr} where ${lr.id} = ${id}"""
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
      create(l.id, l.wfid, l.parent, l.child, l.createdAt)(session)
  }

  def create(
    id: Int,
    wfid: Int,
    parent: Int,
    child: Int,
    createdAt: ZonedDateTime)(implicit session: DBSession): LinkRun = {
    sql"""
      insert into ${LinkRun.table} (
        ${column.id},
        ${column.wfid},
        ${column.parent},
        ${column.child},
        ${column.createdAt}
      ) values (
        ${id},
        ${wfid},
        ${parent},
        ${child},
        ${createdAt}
      )
      """.update.apply()

    LinkRun(
      id = id,
      wfid = wfid,
      parent = parent,
      child = child,
      createdAt = createdAt)
  }

  def batchInsert(entities: collection.Seq[LinkRun])(implicit session: DBSession): List[Int] = {
    val params: collection.Seq[Seq[(String, Any)]] = entities.map(entity =>
      Seq(
        "id" -> entity.id,
        "wfid" -> entity.wfid,
        "parent" -> entity.parent,
        "child" -> entity.child,
        "createdAt" -> entity.createdAt))
    SQL("""insert into link_run(
      id,
      wfid,
      parent,
      child,
      created_at
    ) values (
      {id},
      {wfid},
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
        ${column.id} = ${entity.id},
        ${column.wfid} = ${entity.wfid},
        ${column.parent} = ${entity.parent},
        ${column.child} = ${entity.child},
        ${column.createdAt} = ${entity.createdAt}
      where
        ${column.id} = ${entity.id}
      """.update.apply()
    entity
  }

  def destroy(entity: LinkRun)(implicit session: DBSession): Int = {
    sql"""delete from ${LinkRun.table} where ${column.id} = ${entity.id}""".update.apply()
  }

}

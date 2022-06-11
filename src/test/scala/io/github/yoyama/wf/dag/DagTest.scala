package io.github.yoyama.wf.dag

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers

class DagTest extends AnyFlatSpec with OptionValues
{
  val dagops = new DagOps {
  }

  val root = dagops.createCell(0)
  val terminal = dagops.createCell(-1)
  val cell1 = DagCell(1, 0, Instant.now())
  val dag: Dag = dagops.createDag(root, terminal, cell1)

  "getCell" should "work" in {
  }

  "addCell" should "work" in {
    val cell2 = dagops.createCell(2)
    val dag2 = dagops.addCell(dag, cell1.id, cell2, true).get
    assert(dag2.cells.get(2).value == cell2)
    assert(dag2.children.get(2).value.exists(i => i == terminal.id))
    assert(dag2.parents.get(2).value.exists(i => i == cell1.id))
  }

  "validate" should "work" in {
    val cell2 = dagops.createCell(2)
    val dag2 = dagops.addCell(dag, cell1.id, cell2, true)
    val cell3 = dagops.createCell(3)
    val dag3 = dagops.addCell(dag2.get, cell1.id, cell3, true)
    val result1 = dagops.validate(dag3.get)
    println(result1)
    assert(result1.isSuccess)

    // Add terminal -> root link
    val result2 = dagops.validate(
      dag3.get.copy(
        children = dag3.get.children.updated(terminal.id, Set(root.id)),
        parents = dag3.get.children.updated(root.id, Set(terminal.id))
      ))
    println(result2)
    assert(result2.isFailure)
  }
}

package io.github.yoyama.wf.dag

class DagTest extends munit.FunSuite {
  val dagops = new DagOps {
    type CellData = String
    override def emptyCellData: CellData = ""
  }

  test("validate") {
    val root = dagops.createCell(0, "root")
    val terminal = dagops.createCell(-1, "terminal")
    val cell1 = dagops.DagCell(1, dagops.CellState(), "cell1")
    val dag = dagops.createDag(root, terminal, cell1)
    val cell2 = dagops.DagCell(2, dagops.CellState(), "cell2")
    val dag2 = dagops.addCell(dag, cell1.id, cell2, true)
    val cell3 = dagops.DagCell(3, dagops.CellState(), "cell3")
    val dag3 = dagops.addCell(dag2.get, cell1.id, cell3, true)
    val result1 = dagops.validate(dag3.get)
    println(result1)
    assertEquals(result1.isSuccess, true)
    val result2 = dagops.validate(dag3.get.copy(children = dag3.get.children.updated(-1, Set(0))))
    println(result2)
    assertEquals(result2.isFailure, true)
  }
}

package io.github.yoyama.wf.dag
import java.time.Instant
import scala.util.{Failure, Success, Try}
import io.github.yoyama.utils.OptionHelper


trait DagOps {
  type CellData

  def emptyCellData:CellData

  case class DagDuplicatedCellException(id:Int) extends RuntimeException
  case class DagCellNotFoundException(id:Int) extends RuntimeException
  case class CellState(state: Int = 0, createdAt: Instant = Instant.now())

  case class DagCell(id: Int, state: CellState, data: CellData)

  case class Dag(root: DagCell, terminal: DagCell, cells: Map[Int, DagCell], parents: Map[Int, Set[Int]], children: Map[Int, Set[Int]]) {
    def getCell(id:Int):Option[DagCell] = cells.get(id)
    def getParents(id:Int):Set[DagCell] = {
      val p:Set[Int] = parents.getOrElse(id, Set.empty[Int])
      val p2:Set[DagCell] = p.map(i => cells.get(i).get) // assume Dag is validated
      p2
    }
    def getChildren(id:Int):Set[DagCell] = {
      val c:Set[Int] = children.getOrElse(id, Set.empty[Int])
      val c2:Set[DagCell] = c.map(i => cells.get(i).get) // assume Dag is validated
      c2
    }
  }

  def createCell(id:Int, data:CellData):DagCell = {
    DagCell(id, CellState(), data)
  }

  /**
  def createEmpty(rootId: Int, terminalId: Int): Dag = {
    val root = DagCell(rootId, CellState(0, Instant.now()), emptyCellData)
    val terminal = DagCell(terminalId, CellState(0, Instant.now()), emptyCellData)
    val cells = Map(rootId -> root, terminalId -> terminal)
    val parents = Map[Int, Set[Int]](rootId -> Set.empty, terminalId -> Set(rootId))
    val children = Map[Int, Set[Int]](rootId -> Set(terminalId), terminalId -> Set.empty)
    Dag(root, terminal, cells, parents, children)
  }
  */

  def createDag(root:DagCell, terminal:DagCell, firstCell:DagCell): Dag = {
    val cells = Map(root.id -> root, terminal.id -> terminal, firstCell.id -> firstCell)
    val parents: Map[Int, Set[Int]] = Map(firstCell.id -> Set(root.id), terminal.id -> Set(firstCell.id))
    val children: Map[Int, Set[Int]] = Map(root.id -> Set(firstCell.id), firstCell.id -> Set(terminal.id))
    Dag(root, terminal, cells, parents, children)
  }

  def addCell(dag: Dag, parent: Int, cell: DagCell, terminalProc:Boolean = true): Try[Dag] = {
    val newCells = addCell(dag.cells, cell)
    for {
      newCells <- addCell(dag.cells, cell)
      parentCell <- dag.getCell(parent).toTry(DagCellNotFoundException(parent))
      p1 <- link(dag.parents, cell.id, parent)
      p2 <- if (terminalProc) unlink(p1, dag.terminal.id, parent) else Success(p1)
      newParents <- if (terminalProc) link(p2, dag.terminal.id, cell.id) else Success(p2)
      c1 <- link(dag.children, parent, cell.id)
      c2 <- if (terminalProc) unlink(c1, parent, dag.terminal.id) else Success(c1)
      newChildren <- if (terminalProc) link(c2, cell.id, dag.terminal.id) else Success(c2)
    } yield Dag(dag.root, dag.terminal, newCells, newParents, newChildren)
  }

  def insertCell(dag:Dag, parent: Int, child: Int, cell: DagCell): Try[Dag] = ???

  def linkCells(dag:Dag, parent:Int, child:Int):Try[Dag] = {
    for {
      c1 <- link(dag.children, parent, child)
      p1 <- link(dag.parents, child, parent)
    } yield dag.copy(children = c1, parents = p1)
  }

  def validate(dag:Dag): Try[Dag] = {
    //ToDo check cells without parent except for root
    //ToDo check cells without children except for terminal
    for {
      sortedCells <- sorted(dag)
    } yield(dag)
  }

  def sorted(dag: Dag): Try[Seq[DagCell]] = {
    sort(Seq(dag.root), Seq.empty, dag.cells, dag.children, dag.parents)
  }

  private def sort(noinput:Seq[DagCell], sorted:Seq[DagCell], cells:Map[Int, DagCell], children:Map[Int, Set[Int]], parents:Map[Int, Set[Int]]): Try[Seq[DagCell]] = {
    println(s"noinput:${noinput}, sorted:${sorted}, cells:${cells}")
    println("")
    def getChildren(id:Int): Try[Set[DagCell]] = {
      import cats.implicits._

      children.get(id) match {
        case None => Success(Set.empty)
        case Some(cl) => {
          val v = cl.toSeq.map(c => cells.get(c).toTry(DagCellNotFoundException(c)))
          v.sequence.map(_.toSet)
        }
      }
    }
    // Remove link between pid <-> cells from parents
    def removeParents(pid:Int, cells:Set[DagCell]): Try[Map[Int,Set[Int]]] = {
      //ToDo check existence
      val ret = cells.foldLeft(parents){ (acc, c) =>
        acc.get(c.id).map(x => acc.updated(c.id, x - pid)).getOrElse(acc)
      }
      Success(ret)
    }

    // Remove link between id <-> its children
    def removeChildren(id:Int): Try[Map[Int,Set[Int]]] = {
      val ret = children.get(id) match {
        case None => children.updated(id, Set.empty)
        case Some(v) => children.updated(id, Set.empty)
      }
      Success(ret)
    }

    def remove[V](id:Int, cells:Map[Int, V]):Try[Map[Int, V]] = {
      cells.get(id) match {
        case None => Failure(DagCellNotFoundException(id))
        case Some(_) => Success(cells.removed(id))
      }
    }

    noinput match {
      case Nil if (cells.size == 0) => Success(sorted)
      case Nil => Failure(new RuntimeException("Invalid DAG"))
      case x::xs => {
        val ret = for {
          cells2 <- remove(x.id, cells)
          sorted2 <- Success(sorted :+ x)
          xChildren <- getChildren(x.id)
          children2 <- removeChildren(x.id)
          parents2 <- removeParents(x.id, xChildren)
          noinput2 <- Success(xs ++ xChildren.filter(xc => parents2.get(xc.id) match {
            case None => true
            case Some(v) if( v.size == 0) => true
            case _ => false
          }))
          ret <- sort(noinput2, sorted2, cells2, children2, parents2)
        } yield ret
        ret
      }
    }
  }

  private def addCell(cells:Map[Int,DagCell], cell:DagCell):Try[Map[Int,DagCell]] = {
    cells.get(cell.id) match {
      case Some(v) => throw DagDuplicatedCellException(v.id)
      case None => Success(cells + (cell.id -> cell))
    }
  }

  private def link(links:Map[Int,Set[Int]], p:Int, c:Int): Try[Map[Int,Set[Int]]] = {
    val children = links.get(p)
    children.match {
      case Some(cl) => Success(links + (p -> (cl + c)))
      case None => Success(links + (p -> Set(c)))
    }
  }

  private def unlink(links:Map[Int,Set[Int]], p:Int, c:Int): Try[Map[Int,Set[Int]]] = {
    val children = links.get(p)
    children.match {
      case Some(cl) => {
        Success(links + (p -> (cl - c)))
      }
      case None => {
        Success(links)
      }
    }
  }

}

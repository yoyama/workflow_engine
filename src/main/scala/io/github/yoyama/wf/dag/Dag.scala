package io.github.yoyama.wf.dag
import java.time.Instant
import scala.util.{Failure, Success, Try}
import io.github.yoyama.utils.OptionHelper

trait DagOps {
  type CellID = Int
  type Id2Cell = Map[CellID,DagCell]
  type LINK = Map[Int,Set[Int]]

  case class DagDuplicatedCellException(id:CellID) extends RuntimeException
  case class DagCellNotFoundException(id:CellID) extends RuntimeException
  case class CellState(state: Int = 0, createdAt: Instant = Instant.now())

  case class DagCell(id: CellID, state: CellState)

  case class Dag(root: DagCell, terminal: DagCell, cells: Id2Cell, parents: LINK, children: LINK) {
    def getCell(id:CellID):Option[DagCell] = cells.get(id)

    def getParents(id:CellID):Set[DagCell] = {
      val p:Set[Int] = parents.getOrElse(id, Set.empty[Int])
      val p2:Set[DagCell] = p.map(i => cells.get(i).get) // assume Dag is validated
      p2
    }

    def getChildren(id:CellID):Set[DagCell] = {
      val c:Set[Int] = children.getOrElse(id, Set.empty[Int])
      val c2:Set[DagCell] = c.map(i => cells.get(i).get) // assume Dag is validated
      c2
    }
  }

  def createCell(id:CellID):DagCell = {
    DagCell(id, CellState())
  }

  def createDag(root:DagCell, terminal:DagCell, firstCell:DagCell): Dag = {
    val cells = Map(root.id -> root, terminal.id -> terminal, firstCell.id -> firstCell)
    val parents: Map[Int, Set[Int]] = Map(firstCell.id -> Set(root.id), terminal.id -> Set(firstCell.id))
    val children: Map[Int, Set[Int]] = Map(root.id -> Set(firstCell.id), firstCell.id -> Set(terminal.id))
    Dag(root, terminal, cells, parents, children)
  }

  def getCell(dag: Dag, id:CellID): Try[DagCell] = dag.getCell(id).toTry(DagCellNotFoundException(id))

  def addCell(dag: Dag, parent: CellID, cell: DagCell, terminalProc:Boolean = true): Try[Dag] = {
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

  def insertCell(dag:Dag, parent: CellID, child: CellID, cell: DagCell): Try[Dag] = ???

  def linkCells(dag:Dag, parent:CellID, child:CellID):Try[Dag] = {
    for {
      c <- getCell(dag, child)
      p <- getCell(dag, parent)
      cl <- link(dag.children, parent, child)
      pl <- link(dag.parents, child, parent)
    } yield dag.copy(children = cl, parents = pl)
  }

  def unlinkCells(dag:Dag, parent:CellID, child:CellID):Try[Dag] = {
    for {
      c <- getCell(dag, child)
      p <- getCell(dag, parent)
      cl <- unlink(dag.children, parent, child)
      pl <- unlink(dag.parents, child, parent)
    } yield dag.copy(children = cl, parents = pl)
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

  private def sort(noinput:Seq[DagCell], sorted:Seq[DagCell], cells:Id2Cell, children:LINK, parents:LINK): Try[Seq[DagCell]] = {
    println(s"noinput:${noinput}, sorted:${sorted}, cells:${cells}")
    println("")
    def getChildren(id:CellID): Try[Set[DagCell]] = {
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
    def removeParents(pid:CellID, cells:Set[DagCell]): Try[LINK] = {
      //ToDo check existence
      val ret = cells.foldLeft(parents){ (acc, c) =>
        acc.get(c.id).map(x => acc.updated(c.id, x - pid)).getOrElse(acc)
      }
      Success(ret)
    }

    // Remove link between id <-> its children
    def removeChildren(id:CellID): Try[LINK] = {
      val ret = children.get(id) match {
        case None => children.updated(id, Set.empty)
        case Some(v) => children.updated(id, Set.empty)
      }
      Success(ret)
    }

    def remove[V](id:CellID, cells:Map[Int, V]):Try[Map[Int, V]] = {
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

  private def addCell(cells:Id2Cell, cell:DagCell):Try[Id2Cell] = {
    cells.get(cell.id) match {
      case Some(v) => throw DagDuplicatedCellException(v.id)
      case None => Success(cells + (cell.id -> cell))
    }
  }

  private def link(links:LINK, p:CellID, c:CellID): Try[LINK] = {
    val children = links.get(p)
    children.match {
      case Some(cl) => Success(links + (p -> (cl + c)))
      case None => Success(links + (p -> Set(c)))
    }
  }

  private def unlink(links:LINK, p:CellID, c:CellID): Try[LINK] = {
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

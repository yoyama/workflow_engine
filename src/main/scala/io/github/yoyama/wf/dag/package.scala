package io.github.yoyama.wf
package object dag {
  type CellID = Int
  type Id2Cell = Map[CellID,DagCell]
  type LinkMap = Map[Int,Set[Int]]
}

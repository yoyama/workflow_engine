package io.github.yoyama

import io.github.yoyama.wf.dag.CellID
package object wf {
  type RunID = Int
  type TaskID = CellID // task_run.task_id == dag.CellID
}

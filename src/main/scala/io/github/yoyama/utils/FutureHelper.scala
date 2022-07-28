package io.github.yoyama.utils

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import java.util.concurrent.{ExecutionException, Future as JavaFuture}

object FutureHelper {
  extension[T] (jf: JavaFuture[T]) {
    def toFuture()(implicit ec: ExecutionContext): Future[T] = {
      Future(jf.get()).transform {
        case Failure(e: ExecutionException) =>
          Failure(e.getCause)
        case x => x
      }
    }
  }
}

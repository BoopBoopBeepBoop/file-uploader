package uploader

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.ClosedShape
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.scaladsl.{Broadcast, Compression, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.util.ByteString

import java.math.BigInteger
import java.nio.file.Path
import java.security.MessageDigest
import scala.collection.immutable.ListMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object UploaderFlows {

  /**
   * A flow that will merge file change events with a timer, and buffer changes until those files
   * have stopped changing for some period of time before proceeding
   */
  def timeBufferingFileChangeFlow(
      fileTypeFilter: Path => Boolean,
      fileNotChangedInterval: FiniteDuration
  ): Flow[(Path, DirectoryChange), Path, NotUsed] = {

    val filteredChanges =
      Flow[(Path, DirectoryChange)]
        .filter {
          case (path, dirChange) =>
            fileTypeFilter(path) && (
              dirChange == DirectoryChange.Creation ||
                dirChange == DirectoryChange.Modification
              )
        }.map(Right(_))

    // send a check message down the stream every X amount of time
    case object Check
    val timer = Source.tick(0.seconds, fileNotChangedInterval / 5, Left(Check))

    val bufferedUntilChangesStop: Flow[(Path, DirectoryChange), Path, NotUsed] =
      filteredChanges
        .merge(timer)
        .statefulMapConcat[Path] { () =>
          // this could be done more efficiently with a custom graph stage, but
          // this is sufficient to prove the process. Inbound changes are continually
          // added to the ListMap, causing subsequent changes to move the paths to
          // the back of the list (traversal in iteration order)
          var lastChangeTimes = ListMap.empty[Path, Long]

          {
            case Right((path, _)) =>
              lastChangeTimes += path -> System.currentTimeMillis()
              Nil

            case Left(Check) =>
              val now = System.currentTimeMillis()
              // partition based on the change times, set our map to be the paths that are
              // too new to process, and emit the paths that have exceeded our timer
              val (older, newer) =  lastChangeTimes.span { case (_, time) =>
                now - time > fileNotChangedInterval.toMillis
              }
              lastChangeTimes = newer
              older.keys
          }
        }

    bufferedUntilChangesStop
  }

  /**
   * A flow that groups items by their hashcode and then forces single element processing within
   * each subflow. Useful for linearization of stream elements by a given key. This is the same
   * type of guarantee that you get with kafka.
   *
   * See: https://doc.akka.io/docs/akka/current/stream/stream-substream.html
   */
  def simpleHashcodeGrouping[I, O](parallel: Int)(f: I => Future[O]): Flow[I, O, NotUsed] = {
    Flow[I]
      .groupBy(parallel, _.hashCode % parallel)
      .mapAsync(1)(f)
      .mergeSubstreams
  }

  /**
   * A stream operation that calculates the md5 of byte chunks while gzip compressing them,
   * pulling from the given source & outputting to the given sink. A Sink with a
   * materialization of [[scala.concurrent.Future]] is required so we can flatmap across
   * and provide the output materialization in a convenient final tuple.
   *
   * @param byteInputDef byte source to read from
   * @param gzByteOutputDef gzipped content will be emitted to this
   */
  def combinedMd5Gzip[M1, M2](
      byteInputDef: Source[ByteString, M1],
      gzByteOutputDef: Sink[ByteString, Future[M2]])(
      implicit system: ActorSystem[_]
  ): Future[(String, M2)] = {
    implicit val ctx: ExecutionContext = system.executionContext

    def md5() = java.security.MessageDigest.getInstance("md5")

    // using a lazySink here ensures that we end up with a different md5()
    // digest invocation for each potential instantiation - otherwise our
    // zero value would be computed eagerly, as they are assumed to be immutable
    val digestSinkDef = Sink.lazySink[ByteString, Future[MessageDigest]] { () =>
      Sink.fold[MessageDigest, ByteString](md5()) { (digest, next) =>
        digest.update(next.toArray)
        digest
      }
    }.mapMaterializedValue(_.flatten)

    val graph = RunnableGraph.fromGraph(GraphDSL.createGraph(byteInputDef, digestSinkDef, gzByteOutputDef)((_, _, _)) { implicit builder =>
      (fileSource, digestSink, outputGz) =>
        import GraphDSL.Implicits._

        val compression = Compression.gzip
        val broadcast = builder.add(Broadcast[ByteString](2))

        fileSource ~> broadcast.in

        broadcast ~> digestSink
        broadcast ~> compression ~> outputGz

        ClosedShape
    })

    val (_, digestFuture, ioFuture) = graph.run()

    for {
      digest <- digestFuture
      io <- ioFuture
    } yield {
      val finalDigestBytes = digest.digest()
      val finalDigestBigInt = new BigInteger(1, finalDigestBytes)
      val finalDigestString = String.format("%032X", finalDigestBigInt).toLowerCase()
      println(s"Digest: $finalDigestString IO: $io")
      (finalDigestString, io)
    }
  }
}

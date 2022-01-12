package uploader

import akka.Done
import akka.actor.typed.scaladsl.Behaviors._
import akka.actor.typed.{ActorSystem, Behavior, ChildFailed, DispatcherSelector}
import akka.stream.ClosedShape
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.scaladsl.{Broadcast, Compression, FileIO, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString

import java.math.BigInteger
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.security.MessageDigest
import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

object Run extends App {
  val system = ActorSystem(MainActor(), "file-uploader")
  system.whenTerminated.map { _ => sys exit 0 } (ExecutionContext.global)
}

object MainActor {
  def apply(): Behavior[Any] = setup { ctx =>
    import ctx._
    println("starting main")
    watch(spawn(Streamer(), "streamer"))

    receiveSignal[Any] {
      case (_, ChildFailed(c, e)) =>
        e.printStackTrace()
        throw e
    }
  }
}

object Streamer {

  def apply(): Behavior[Any] = setup { ctx =>
    implicit val system: ActorSystem[_] = ctx.system
    implicit val exec: ExecutionContext = ctx.executionContext
    val blocking: ExecutionContext =
      ExecutionContext.fromExecutor(ctx.system.dispatchers.lookup(DispatcherSelector.blocking()))

    val path = Paths.get(sys.props("user.dir"))
    val fileTypeFilter = ".csv"
    val changes = DirectoryChangesSource(
      path,
      pollInterval = 1.seconds,
      maxBufferSize = 1000)

    // filter to only include changes that we care about
    val filteredChanges =
      changes
        .filter {
          case (path, dirChange) =>
            path.toString.endsWith(fileTypeFilter) && (
              dirChange == DirectoryChange.Creation ||
              dirChange == DirectoryChange.Modification
            )
        }.map(Right(_))

    // send a check message down the stream every X amount of time
    case object Check
    val timer = Source.tick(0.seconds, 300.millis, Left(Check))

    val bufferedUntilChangesStop =
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
              val (older, newer) =  lastChangeTimes.span { case (_, time) => now - time > 1000 }
              lastChangeTimes = newer
              older.keys
          }
        }

    def md5() = java.security.MessageDigest.getInstance("md5")
    def rand(name: String) = s"$name-${Random.alphanumeric.take(6).mkString}"

    val maxParallel = 100
    val substreamParallelism = 1

    val parallelFileMoves =
      bufferedUntilChangesStop
        // dumb hash implementation is sufficient to ensure same keys
        // end up on same substream - this is the same type of partitioning
        // guarantee that kafka relies on to ensure ordering. As a result,
        // this is slightly less "fair" but ensures that we will single-thread
        // writes to the same file. See substream docs at
        // https://doc.akka.io/docs/akka/current/stream/stream-substream.html
        .groupBy(maxParallel, _.toString.hashCode % maxParallel)
        .mapAsync(substreamParallelism) { path =>
          Future { Try {
            val tmpPath = path.getParent.resolve(rand(path.getFileName.toString))
            val gzPath = path.getParent.resolve(rand(path.getFileName.toString))

            println(s"Tmp: $tmpPath")
            println(s"Gz: $gzPath")

            Files.copy(path, tmpPath, StandardCopyOption.COPY_ATTRIBUTES)
            (path, tmpPath, gzPath)
          }}(blocking)
        }.mapAsync(substreamParallelism) {
          case Success((_, tmpPath, gzPath)) =>

            // using a lazySink here ensures that we end up with a different md5()
            // digest invocation for each potential instantiation - otherwise our
            // zero value would be computed eagerly, as they are assumed to be immutable
            val digestSinkDef = Sink.lazySink[ByteString, Future[MessageDigest]] { () =>
              Sink.fold[MessageDigest, ByteString](md5()) { (digest, next) =>
                digest.update(next.toArray)
                digest
              }
            }.mapMaterializedValue(_.flatten)

            val outputGzDef = FileIO.toPath(gzPath)
            val fileSourceDef = FileIO.fromPath(tmpPath)

            val graph = RunnableGraph.fromGraph(GraphDSL.createGraph(fileSourceDef, digestSinkDef, outputGzDef)((_, _, _)) { implicit builder =>
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
              Right((finalDigestString, io))
            }

          case Failure(e) =>
            println(e.printStackTrace())
            Future.successful(Left(e))
        }
        .mergeSubstreams

    ctx.pipeToSelf(parallelFileMoves.run())(identity)

    receiveMessagePartial {
      case Success(Done) =>
        println("stream finished?")
        stopped

      case Failure(e) =>
        throw new RuntimeException("Stream failed: ", e)
    }
  }
}

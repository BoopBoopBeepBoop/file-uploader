package uploader

import akka.Done
import akka.actor.typed.scaladsl.Behaviors._
import akka.actor.typed.{ActorSystem, Behavior, ChildFailed, DispatcherSelector}
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.alpakka.s3.{AccessStyle, MetaHeaders, MultipartUploadResult, S3Attributes, S3Headers, S3Settings}
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{FileIO, Keep}

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
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

    val rootPath = Paths.get(sys.props("user.dir"))
    val fileTypeFilter = (p: Path) => p.toString.endsWith(".csv")
    val maxParallel = 100
    val bucket = "test"

    val flow =
      DirectoryChangesSource(rootPath, pollInterval = 1.seconds, maxBufferSize = 1000)
        .via(UploaderFlows.timeBufferingFileChangeFlow(
          fileTypeFilter,
          fileNotChangedInterval = 1.second
        ))
        .via(
          UploaderFlows.simpleHashcodeGrouping(maxParallel) { path: Path =>
            copyAndMakeTmpFiles(path)(blocking)
              .flatMap {
                case Success((path, tmpPath, gzPath)) =>
                  val subPathKey = rootPath.relativize(path).toString
                  calculateAllAndUpload(bucket, subPathKey, tmpPath, gzPath, blocking)
                    .recover {
                      case e => e.printStackTrace()
                        throw e
                    }

                case Failure(e) =>
                  println(e.printStackTrace())
                  throw e
//                  Future.successful(Left(e))
              }
          }
        )


    ctx.pipeToSelf(flow.run())(identity)

    receiveMessagePartial {
      case Success(Done) =>
        println("stream finished?")
        stopped

      case Failure(e) =>
        throw new RuntimeException("Stream failed: ", e)
    }
  }

  private def calculateAllAndUpload(
      uploadBucket: String,
      uploadKey: String,
      tmpPath: Path,
      gzPath: Path,
      blocking: ExecutionContext)(
      implicit exec: ExecutionContext,
      system: ActorSystem[_]
  ): Future[MultipartUploadResult] = {
    val uploadOperation =
      UploaderFlows.combinedMd5Gzip(
        byteInputDef = FileIO.fromPath(tmpPath),
        gzByteOutputDef = FileIO.toPath(gzPath)
      ).flatMap { case (digest, ioResult) =>

        def s3Upload =
          S3.multipartUploadWithHeaders(
            uploadBucket,
            uploadKey,
            s3Headers = S3Headers()
              .withCustomHeaders(Map("Content-encoding" -> "gzip"))
              .withMetaHeaders(MetaHeaders(Map("raw_checksum" -> digest)))
          ).withAttributes(
            S3Attributes.settings(
              S3Settings.apply()
                .withAccessStyle(AccessStyle.PathAccessStyle)
                .withEndpointUrl("http://localhost:9000")
            ))

        FileIO
          .fromPath(gzPath)
          .toMat(s3Upload)(Keep.right)
          .run()
          .map { result =>
            println(s"File uploaded to ${result.bucket}/${result.key} v: ${result.versionId}")
            result
          }
      }

    uploadOperation.onComplete { fin =>
      Files.deleteIfExists(tmpPath)
      Files.deleteIfExists(gzPath)
    }(blocking)

    uploadOperation
  }

  def copyAndMakeTmpFiles(path: Path)(exec: ExecutionContext): Future[Try[(Path, Path, Path)]] = {
    def rand(name: String, ext: String) = s"$name-${Random.alphanumeric.take(6).mkString}.$ext"

    Future { Try {
      val dir = Files.createTempDirectory("uploader")
      val tmpPath = dir.resolve(rand(path.getFileName.toString, "tmp"))
      val gzPath = dir.resolve(rand(path.getFileName.toString, "gz"))

      println(s"Tmp: $tmpPath")
      println(s"Gz: $gzPath")

      Files.copy(path, tmpPath, StandardCopyOption.COPY_ATTRIBUTES)
      (path, tmpPath, gzPath)
    }}(exec)
  }

}

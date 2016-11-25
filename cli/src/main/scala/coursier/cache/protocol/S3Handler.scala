package coursier.cache.protocol

import awscala.s3.Bucket
import coursier.Platform
import coursier.core.{Module, Project, Version}
import coursier.core.compatibility.xmlParse
import coursier.maven.Pom
import java.io.{ByteArrayInputStream, InputStream}
import java.net.{URL, URLConnection, URLStreamHandler, URLStreamHandlerFactory}

class S3Handler extends URLStreamHandlerFactory {

  override def createURLStreamHandler(protocol: String): URLStreamHandler =
    new URLStreamHandler {

      private[this] val fallback =
        new S3cHandler

      override def openConnection(url: URL): URLConnection = {
        val fullPath = url.getPath
        val subPaths = fullPath.split("/").map(_.trim).filter(_.nonEmpty)
        (subPaths.headOption, subPaths.lastOption) match {
          case (Some(bucketName), Some("maven-metadata.xml")) =>
            val endpoint = Option(url.getPort).filter(_ > 0).map(p => s"${url.getHost}:$p").getOrElse(url.getHost)
            S3Client.setEndpoint(endpoint)
            val key = subPaths.tail.mkString("/")
            if (S3Client.instance.doesObjectExist(bucketName, key)) {
              fallback.openConnection(url)
            } else {
              val bucket = Bucket(bucketName)
              val prefix = subPaths.tail.dropRight(1).mkString("", "/", "/")
              val keys   = S3Client.instance.keys(bucket, prefix)
              val poms   = keys.collect({ case PomKey(k, m, v) => (k, m, v) })
              if (poms.isEmpty) {
                fallback.openConnection(url)
              } else {
                parsePom(S3Client.prepareFileContents(bucket, poms.head._1)) match {
                  case Right(project) =>
                    val metadata = buildMavenMetadata(project.module, poms.map(_._3))
                    new S3Handler.FakeConnection(url, metadata)
                  case Left(message) =>
                    throw new Exception(message)
                }
              }
            }
          case (_, _) =>
            fallback.openConnection(url)
        }
      }

    }

  private[this] object PomKey {
    def unapply(key: String): Option[(String, String, Version)] =
      key.split('/').takeRight(3) match {
        case Array(mod, ver, pom) if mod.nonEmpty && ver.nonEmpty && pom.endsWith(".pom") =>
          Some((key, mod, Version(ver)))
        case _ =>
          None
      }
  }

  private[this] def buildMavenMetadata(module: Module, versions: Seq[Version]): String = {
    val sorted = versions.sorted
    s"""<metadata modelVersion="1.1.0">
       |  <groupId>${module.organization}</groupId>
       |  <artifactId>${module.name}</artifactId>
       |  <versioning>
       |    <latest>${sorted.last.repr}</latest>
       |    <release>${sorted.last.repr}</release>
       |    <versions>${sorted.map(v => s"<version>${v.repr}</version>").mkString("")}</versions>
       |    <lastUpdated>${System.currentTimeMillis / 1000L}</lastUpdated>
       |  </versioning>
       |</metadata>
       |""".stripMargin
  }

  private[this] def parsePom(s: => InputStream): Either[String, Project] =
    Platform.readFully(s)
      .unsafePerformSyncAttempt
      .leftMap(_.getMessage)
      .flatMap(identity)
      .toEither.right
      .flatMap(xmlParse).right
      .flatMap(Pom.project(_).toEither)

}

object S3Handler {

  class FakeConnection(url: URL, contents: String) extends URLConnection(url) {

    override def connect(): Unit =
      ()

    override def getInputStream: InputStream =
      new ByteArrayInputStream(contents.getBytes("UTF-8"))

  }

}

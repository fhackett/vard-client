package com.github.fhackett.vardclient

import com.github.fhackett.vardclient.VardClient.NotLeaderError

import java.math.BigInteger
import java.net.{InetSocketAddress, SocketException}
import java.nio.{ByteBuffer, ByteOrder, CharBuffer}
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

final class VardClient private[vardclient](config: VardClientBuilder) extends AutoCloseable {
  private val endpointAddrs = config.endpoints.map {
    case (host, port) => new InetSocketAddress(host, port)
  }
  private val clientId: String = config.clientId.getOrElse(
    new Random().nextInt(Int.MaxValue).toString)

  private implicit final class ChannelHelpers(channel: SocketChannel) {
    def writeAll(buffer: ByteBuffer): Unit = {
      val expectedBytes = buffer.remaining()
      val actualBytes = Iterator.continually {
        channel.write(buffer)
      }
        .scanLeft(0)(_ + _)
        .find(_ == expectedBytes)
        .get
      assert(expectedBytes == actualBytes)
    }

    def readAll(buffer: ByteBuffer): Unit = {
      val expectedBytes = buffer.remaining()
      val actualBytes = Iterator.continually {
        channel.read(buffer)
      }
        .scanLeft(0)(_ + _)
        .find(_ == expectedBytes)
        .get
      assert(expectedBytes == actualBytes)
    }
  }

  private object socket {
    private val random = new Random()
    private var socketChannel: Option[SocketChannel] = None

    private val lenBuffer = ByteBuffer
      .allocate(4)
      .order(ByteOrder.LITTLE_ENDIAN)

    private def sendImpl(socketChannel: SocketChannel, msg: ByteBuffer): Try[SocketChannel] =
      Try {
        val msgsRemaining = msg.remaining()
        lenBuffer
          .clear()
          .putInt(msgsRemaining)
          .flip()

        socketChannel.writeAll(lenBuffer)
        socketChannel.writeAll(msg)

        socketChannel
      }

    private val encodedClientId =
      StandardCharsets.UTF_8.encode(CharBuffer.wrap(clientId))

    private def endpointsIter: Iterator[InetSocketAddress] = {
      val startIdx = random.between(minInclusive = 0, maxExclusive = endpointAddrs.length)
      endpointAddrs.iterator.drop(startIdx) ++
        endpointAddrs.iterator.take(startIdx)
    }

    private def findSocketChannel[T](fn: SocketChannel => Try[T]): Try[T] = {
      val possibleSockets = endpointsIter
        .map { addr =>
          println(s"try connecting to $addr")
          Try(SocketChannel.open(addr)).map { channel =>
            socketChannel = Some(channel)
            channel
          }
        }

      var lastErr: Option[Throwable] = None

      val resultOpt = (socketChannel.map(Success(_)).iterator ++ possibleSockets)
        .map {  tryChannel =>
          if(config.ivyMode) {
            //println("ivy mode: skip sending client ID, send as prefix instead")
            tryChannel
          } else {
            tryChannel.flatMap { channel =>
              //println("sending client ID...")
              sendImpl(channel, encodedClientId.rewind())
            }
          }
        }
        .map(_.flatMap { channel =>
          fn(channel)
            .map((_, channel))
        })
        .tapEach { tryChannel =>
          if(tryChannel.isFailure) {
            drop()
          }
        }
        .find {
          case Failure(err@NotLeaderError()) =>
            println("not leader...")
            lastErr = Some(err)
            false
          case Failure(err: SocketException) =>
            println("failed to connect:")
            err.printStackTrace()
            lastErr = Some(err)
            false
          case Failure(_) => true
          case Success(_) => true
        }

      resultOpt match {
        case None =>
          throw new AssertionError("could not connect to any endpoints", lastErr.get)
        case Some(tryResult) =>
          tryResult.map {
            case (result, channel) =>
              socketChannel = Some(channel)
              result
          }
      }
    }

    def proc[T](msg: String)(fn: String => Try[T]): Try[T] =
      findSocketChannel { channel =>
        for {
          _ <- sendImpl(channel, StandardCharsets.UTF_8.encode(msg))
          response <- recvImpl(channel)
          result <- fn(response)
        } yield result
      }

    private var responseBuffer: ByteBuffer = ByteBuffer.allocate(64)

    private def recvImpl(channel: SocketChannel): Try[String] =
      Try {
        lenBuffer.clear()
        channel.readAll(lenBuffer)
        val responseLen = lenBuffer.flip().getInt()

        if(responseBuffer.capacity() < responseLen) {
          if(responseBuffer.capacity() * 2 < responseLen) {
            responseBuffer = ByteBuffer.allocate(responseLen)
          } else {
            responseBuffer = ByteBuffer.allocate(responseBuffer.capacity() * 2)
          }
        }
        responseBuffer
          .clear()
          .limit(responseLen)

        channel.readAll(responseBuffer)

        responseBuffer.flip()
        StandardCharsets.UTF_8.decode(responseBuffer)
          .toString
      }

    def drop(): Unit = {
      socketChannel.foreach(_.close())
      socketChannel = None
    }
  }

  private var requestId = 0

  private def safelyEncodeBytes(bytes: ByteBuffer): String = {
    val byteArrayBuilder = mutable.ArrayBuilder.make[Byte]
    byteArrayBuilder += 1
    while(bytes.remaining() > 0) {
      byteArrayBuilder += bytes.get()
    }
    new BigInteger(1, byteArrayBuilder.result())
      .toString(36)
  }

  private def safelyDecodeBytes(str: String): ByteBuffer = {
    val bytes = new BigInteger(str, 36).toByteArray
    ByteBuffer.wrap(bytes, 1, bytes.length - 1)
  }

  import VardClient._

  private val ResponsePattern = raw"Response\W+([0-9]+)\W+([/A-Za-z0-9]+|-)\W+([/A-Za-z0-9]+|-)\W+([/A-Za-z0-9]+|-)".r

  private val clientIdPrefix = if(config.ivyMode) s"$clientId " else ""

  def put(key: ByteBuffer, value: ByteBuffer): Try[Unit] = {
    key.mark(); value.mark()
    requestId += 1
    socket.proc(s"$clientIdPrefix$requestId PUT ${safelyEncodeBytes(key)} ${safelyEncodeBytes(value)} -") {
      case s"NotLeader$_" => Failure(NotLeaderError())
      case ResponsePattern(requestIdStr, _, _, _) =>
        assert(requestIdStr.toInt == requestId)
        Success(())
    }
  }

  def get(key: ByteBuffer): Try[ByteBuffer] = {
    key.mark()
    requestId += 1
    socket.proc(s"$clientIdPrefix$requestId GET ${safelyEncodeBytes(key)} - -") {
      case s"NotLeader$_" => Failure(NotLeaderError())
      case ResponsePattern(requestIdStr, _, data, _) =>
        assert(requestIdStr.toInt == requestId)
        if(data == "-") {
          Failure(NotFoundError())
        } else {
          Success(safelyDecodeBytes(data))
        }
    }
  }

  def del(key: ByteBuffer): Try[Unit] = {
    key.mark()
    requestId += 1
    socket.proc(s"$clientIdPrefix$requestId DEL ${safelyEncodeBytes(key)} - -") {
      case s"NotLeader$_" => Failure(NotLeaderError())
      case ResponsePattern(requestIdStr, _, _, _) =>
        assert(requestIdStr.toInt == requestId)
        Success(())
    }
  }

  override def close(): Unit =
    socket.drop()
}

object VardClient {
  def builder: VardClientBuilder =
    VardClientBuilder(
      endpoints = Nil,
      timeout = 50,
      clientId = None,
      ivyMode = false)

  case class NotLeaderError() extends RuntimeException

  case class NotFoundError() extends RuntimeException

  // -- here lies very basic testing code ---

  import scala.util.Using

  def main(args: Array[String]): Unit = {
    val builder =
      VardClient
        .builder
        .withEndpoints("localhost:8000,localhost:8001")

    Using(builder.build) { client =>
      for {
        _ <- client.put(ByteBuffer.wrap("foo".getBytes), ByteBuffer.wrap("bar".getBytes()))
        buf <- client.get(ByteBuffer.wrap("foo".getBytes()))
        _ = println(StandardCharsets.UTF_8.decode(buf).toString)
      } yield ()
    }
      .get
      .get
  }
}

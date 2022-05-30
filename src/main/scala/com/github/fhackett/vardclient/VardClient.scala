package com.github.fhackett.vardclient

import com.github.fhackett.vardclient.VardClient.NotLeaderError

import java.math.BigInteger
import java.net.{InetSocketAddress, SocketException}
import java.nio.{ByteBuffer, ByteOrder, CharBuffer}
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

final class VardClient private[vardclient](config: VardClientBuilder) extends AutoCloseable {
  private val endpointAddrs = config.endpoints.map {
    case (host, port) => new InetSocketAddress(host, port)
  }
  private val clientId: String = config.clientId.getOrElse(
    UUID.randomUUID().toString.replace("-",""))

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
      .allocateDirect(4)
      .order(ByteOrder.LITTLE_ENDIAN)

    private def sendImpl(socketChannel: SocketChannel, msg: ByteBuffer): Try[SocketChannel] =
      Try {
        val msgsRemaining = msg.remaining()
        lenBuffer
          .clear()
          .putInt(msgsRemaining)
          .flip()

        val expectedBytesWritten = lenBuffer.remaining() + msgsRemaining
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

    private def findSocketChannel(): Try[SocketChannel] =
      socketChannel
        .map(Success(_))
        .getOrElse {
          Try {
            var lastErr: Option[Throwable] = None

            val channelOpt: Option[SocketChannel] = endpointsIter
              .collectFirst(Function.unlift { addr =>
                val result = Try(SocketChannel.open(addr))
                  .flatMap(sendImpl(_, encodedClientId.rewind()))

                result.failed.foreach { err =>
                  lastErr = Some(err)
                }

                result.toOption
              })

            if (channelOpt.isEmpty) {
              throw new AssertionError("could not connect to any endpoints", lastErr.get)
            }

            socketChannel = channelOpt
            channelOpt.get
          }
        }

    def send(msg: String): Try[Unit] =
      findSocketChannel()
        .flatMap(sendImpl(_, StandardCharsets.UTF_8.encode(msg)))
        .map(_ => ())

    private var responseBuffer: ByteBuffer = ByteBuffer.allocateDirect(64)

    def recv(): Try[String] = {
      assert(socketChannel.nonEmpty)
      Try(socketChannel.get)
        .map { socketChannel =>
          lenBuffer.clear()
          socketChannel.readAll(lenBuffer)
          val responseLen = lenBuffer.flip().getInt()

          if(responseBuffer.capacity() < responseLen) {
            if(responseBuffer.capacity() * 2 < responseLen) {
              responseBuffer = ByteBuffer.allocateDirect(responseLen)
            } else {
              responseBuffer = ByteBuffer.allocateDirect(responseBuffer.capacity() * 2)
            }
          }
          responseBuffer
            .clear()
            .limit(responseLen)

          socketChannel.readAll(responseBuffer)

          responseBuffer.flip()
          StandardCharsets.UTF_8.decode(responseBuffer)
            .toString
        }
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

  private val ResponsePattern = raw"Response\W+([0-9]+)\W+([/A-Za-z0-9]+|-)\W+([/A-Za-z0-9]+|-)\W+([/A-Za-z0-9]+|-)".r

  private def performRequest[T](reqFn: Int => Try[T]): Try[T] =
    Iterator.continually {
      requestId += 1
      reqFn(requestId)
    }
      .find {
        case Success(_) => true
        case Failure(NotLeaderError()) => false
        case Failure(_: SocketException) =>
          socket.drop()
          false
        case Failure(_) => true
      }
      .get

  def put(key: ByteBuffer, value: ByteBuffer): Try[Unit] = {
    key.mark(); value.mark()
    performRequest { requestId =>
      key.reset()
      value.reset()
      for {
        _ <- socket.send(s"$requestId PUT ${safelyEncodeBytes(key)} ${safelyEncodeBytes(value)} -")
        resp <- socket.recv()
        _ <- resp match {
          case s"NotLeader$_" =>
            socket.drop()
            Failure(NotLeaderError())
          case ResponsePattern(requestIdStr, _, _, _) =>
            assert(requestIdStr.toInt == requestId)
            Success(())
        }
      } yield ()
    }
  }

  def get(key: ByteBuffer): Try[ByteBuffer] = {
    key.mark()
    performRequest { requestId =>
      key.reset()
      for {
        _ <- socket.send(s"$requestId GET ${safelyEncodeBytes(key)} - -")
        resp <- socket.recv()
        result <- resp match {
          case s"NotLeader$_" =>
            socket.drop()
            Failure(NotLeaderError())
          case ResponsePattern(requestIdStr, _, data, _) =>
            assert(requestIdStr.toInt == requestId)
            Success(safelyDecodeBytes(data))
        }
      } yield result
    }
  }

  def del(key: ByteBuffer): Try[Unit] = {
    key.mark()
    performRequest { requestId =>
      key.reset()
      for {
        _ <- socket.send(s"$requestId DEL ${safelyEncodeBytes(key)} - -")
        resp <- socket.recv()
        _ <- resp match {
          case s"NotLeader$_" =>
            socket.drop()
            Failure(NotLeaderError())
          case ResponsePattern(requestIdStr, _, _, _) =>
            assert(requestIdStr.toInt == requestId)
            Success(())
        }
      } yield ()
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
      clientId = None)

  case class NotLeaderError() extends RuntimeException

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

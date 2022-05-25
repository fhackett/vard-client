package com.github.fhackett.vardclient

import scala.annotation.tailrec

final case class VardClientBuilder private[vardclient] (private[vardclient] val endpoints: List[(String,Int)],
                                                        private[vardclient] val timeout: Int,
                                                        private[vardclient] val clientId: Option[String]) {
  def withEndpoint(host: String, port: Int): VardClientBuilder =
    copy(endpoints = (host, port) +: endpoints)

  private object HostPort {
    private val DigitsRegex = raw"(\d+)".r

    def unapply(candidate: String): Option[(String,Int)] =
      candidate match {
        case s"$host:${DigitsRegex(portStr)}" => Some((host, portStr.toInt))
        case _ => None
      }
  }

  @tailrec
  def withEndpoints(endpoints: String): VardClientBuilder =
    endpoints match {
      case HostPort(host, port) =>
        withEndpoint(host, port)
      case s"${HostPort(host, port)},$restStr" =>
        withEndpoint(host, port).withEndpoints(restStr)
      case badStr =>""
        throw new IllegalArgumentException(s"endpoints should be a non-empty comma-separated list of host:port pairs. could not read past here: \"$badStr\"")
    }

  def withTimeout(timeout: Int): VardClientBuilder =
    copy(timeout = timeout)

  def withClientId(clientId: String): VardClientBuilder =
    copy(clientId = Some(clientId))

  def build: VardClient =
    new VardClient(this)
}

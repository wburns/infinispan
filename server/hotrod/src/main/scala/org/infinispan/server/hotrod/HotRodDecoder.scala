package org.infinispan.server.hotrod

import java.io.{IOException, StreamCorruptedException}
import java.lang.StringBuilder
import java.security.PrivilegedExceptionAction
import javax.security.auth.Subject
import javax.security.sasl.SaslServer
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.handler.codec.ReplayingDecoder
import io.netty.util.CharsetUtil
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.security.Security
import HotRodDecoderState._
import org.infinispan.server.core.Operation._
import org.infinispan.server.core._
import org.infinispan.server.core.logging.Log
import org.infinispan.server.core.security.AuthorizingCallbackHandler
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.core.transport._
import javax.security.sasl.Sasl
import java.security.PrivilegedActionException

import scala.annotation.switch

class UnknownVersionException(reason: String, val version: Byte, val messageId: Long)
extends StreamCorruptedException(reason)

class HotRodUnknownOperationException(reason: String, val version: Byte, val messageId: Long)
extends UnknownOperationException(reason)

class InvalidMagicIdException(reason: String) extends StreamCorruptedException(reason)

class CacheUnavailableException extends Exception

class RequestParsingException(reason: String, val version: Byte, val messageId: Long, cause: Exception)
extends IOException(reason, cause) {
   def this(reason: String, version: Byte, messageId: Long) = this(reason, version, messageId, null)
}

class HotRodHeader {
   var op: NewHotRodOperation = _
   var version: Byte = _
   var messageId: Long = _
   var cacheName: String = _
   var flag: Int = _
   var clientIntel: Short = _
   var topologyId: Int = _

   override def toString = {
      new StringBuilder().append("HotRodHeader").append("{")
      .append("op=").append(op)
      .append(", version=").append(version)
      .append(", messageId=").append(messageId)
      .append(", cacheName=").append(cacheName)
      .append(", flag=").append(flag)
      .append(", clientIntelligence=").append(clientIntel)
      .append(", topologyId=").append(topologyId)
      .append("}").toString
   }
}

class CacheNotFoundException(msg: String, override val version: Byte, override val messageId: Long)
extends RequestParsingException(msg, version, messageId)

class HotRodException(val response: ErrorResponse, cause: Throwable) extends Exception(cause)

class UnknownOperationException(reason: String) extends StreamCorruptedException(reason)

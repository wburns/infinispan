package org.infinispan.server.hotrod

import java.io.IOException
import java.security.PrivilegedActionException
import java.util
import java.util.{BitSet => JavaBitSet, HashMap, HashSet, Map}

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import org.infinispan.IllegalLifecycleStateException
import org.infinispan.commons.CacheException
import org.infinispan.configuration.cache.Configuration
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.versioning.NumericVersion
import org.infinispan.context.Flag.{IGNORE_RETURN_VALUES, SKIP_CACHE_LOAD, SKIP_INDEXING}
import org.infinispan.remoting.transport.jgroups.SuspectException
import org.infinispan.server.core.Operation._
import org.infinispan.server.core._
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.core.transport.NettyTransport
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.stats.ClusterCacheStats
import org.infinispan.util.concurrent.TimeoutException

import scala.annotation.{switch, tailrec}
import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

/**
 * HotRod protocol decoder specific for specification version 2.0.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
object Decoder2x extends AbstractVersionedDecoder with ServerConstants with Log with Constants {

   import OperationResponse._
   import ProtocolFlag._

   type SuitableHeader = HotRodHeader
   private val isTrace = isTraceEnabled

   override def readHeader(buffer: ByteBuf, version: Byte, messageId: Long, header: HotRodHeader, requireAuth: Boolean): Boolean = {
      val streamOp = buffer.readUnsignedByte
      val (op, endOfOp) = (streamOp: @switch) match {
         case 0x01 => (NewHotRodOperation.PutRequest, false)
         case 0x03 => (NewHotRodOperation.GetRequest, false)
         case 0x05 => (NewHotRodOperation.PutIfAbsentRequest, false)
         case 0x07 => (NewHotRodOperation.ReplaceRequest, false)
         case 0x09 => (NewHotRodOperation.ReplaceIfUnmodifiedRequest, false)
         case 0x0B => (NewHotRodOperation.RemoveRequest, false)
         case 0x0D => (NewHotRodOperation.RemoveIfUnmodifiedRequest, false)
         case 0x0F => (NewHotRodOperation.ContainsKeyRequest, false)
         case 0x11 => (NewHotRodOperation.GetWithVersionRequest, false)
         case 0x13 => (NewHotRodOperation.ClearRequest, true)
         case 0x15 => (NewHotRodOperation.StatsRequest, true)
         case 0x17 => (NewHotRodOperation.PingRequest, true)
         case 0x19 => (NewHotRodOperation.BulkGetRequest, false)
         case 0x1B => (NewHotRodOperation.GetWithMetadataRequest, false)
         case 0x1D => (NewHotRodOperation.BulkGetKeysRequest, false)
         case 0x1F => (NewHotRodOperation.QueryRequest, false)
         case 0x21 => (NewHotRodOperation.AuthMechListRequest, true)
         case 0x23 => (NewHotRodOperation.AuthRequest, true)
         case 0x25 => (NewHotRodOperation.AddClientListenerRequest, false)
         case 0x27 => (NewHotRodOperation.RemoveClientListenerRequest, false)
         case 0x29 => (NewHotRodOperation.SizeRequest, true)
         case 0x2B => (NewHotRodOperation.ExecRequest, true)
         case 0x2D => (NewHotRodOperation.PutAllRequest, false)
         case 0x2F => (NewHotRodOperation.GetAllRequest, false)
         case 0x31 => (NewHotRodOperation.IterationStartRequest, false)
         case 0x33 => (NewHotRodOperation.IterationNextRequest, false)
         case 0x35 => (NewHotRodOperation.IterationEndRequest, false)
         case _ => throw new HotRodUnknownOperationException(
            "Unknown operation: " + streamOp, version, messageId)
      }
      if (isTrace) trace("Operation code: %d has been matched to %s", streamOp, op)
      if (requireAuth) {
        op match {
          case NewHotRodOperation.PingRequest | NewHotRodOperation.AuthMechListRequest | NewHotRodOperation.AuthRequest => ;
          case _ => throw log.unauthorizedOperation
        }
      }

      val cacheName = readString(buffer)
      val flag = readUnsignedInt(buffer)
      val clientIntelligence = buffer.readUnsignedByte
      val topologyId = readUnsignedInt(buffer)

      header.op = op
      header.version = version
      header.messageId = messageId
      header.cacheName = cacheName
      header.flag = flag
      header.clientIntel = clientIntelligence
      header.topologyId = topologyId
      endOfOp
   }

   private def readKey(buffer: ByteBuf): Array[Byte] = readRangedBytes(buffer)

   override def readParameters(header: HotRodHeader, buffer: ByteBuf): (RequestParameters, Boolean) = {
      (header.op: @switch) match {
         case NewHotRodOperation.RemoveRequest => (null, true)
         case NewHotRodOperation.RemoveIfUnmodifiedRequest => (new RequestParameters(-1, new ExpirationParam(-1, TimeUnitValue.SECONDS), new ExpirationParam(-1, TimeUnitValue.SECONDS), buffer.readLong), true)
         case NewHotRodOperation.ReplaceIfUnmodifiedRequest =>
            val expirationParams = readLifespanMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan), hasFlag(header, ProtocolFlag.DefaultMaxIdle), header.version)
            val version = buffer.readLong
            val valueLength = readUnsignedInt(buffer)
            (new RequestParameters(valueLength, expirationParams._1, expirationParams._2, version), false)
         case NewHotRodOperation.PutAllRequest =>
            // Since we have custom code handling for valueLength to allocate an array
            // always we have to pass back false and set the checkpoint manually...
            val expirationParams = readLifespanMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan), hasFlag(header, ProtocolFlag.DefaultMaxIdle), header.version)
            val valueLength = readUnsignedInt(buffer)
            (new RequestParameters(valueLength, expirationParams._1, expirationParams._2, -1), true)
         case NewHotRodOperation.GetAllRequest =>
            val count = readUnsignedInt(buffer)
            (new RequestParameters(count, new ExpirationParam(-1, TimeUnitValue.SECONDS), new ExpirationParam(-1, TimeUnitValue.SECONDS), -1), true)
         case _ =>
            val expirationParams = readLifespanMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan), hasFlag(header, ProtocolFlag.DefaultMaxIdle), header.version)
            val valueLength = readUnsignedInt(buffer)
            (new RequestParameters(valueLength, expirationParams._1, expirationParams._2, -1), false)
      }
   }

   private def hasFlag(h: HotRodHeader, f: ProtocolFlag): Boolean = {
      (h.flag & f.id) == f.id
   }

   private def readLifespanMaxIdle(buffer: ByteBuf, usingDefaultLifespan: Boolean, usingDefaultMaxIdle: Boolean, version: Byte): (ExpirationParam, ExpirationParam) = {
      def readDuration(useDefault: Boolean) = {
         val duration = readUnsignedInt(buffer)
         if (duration <= 0) {
            if (useDefault) EXPIRATION_DEFAULT else EXPIRATION_NONE
         } else duration
      }
      def readDurationIfNeeded(timeUnitValue: TimeUnitValue) = {
         if (timeUnitValue.isDefault) EXPIRATION_DEFAULT.toLong
         else {
            if (timeUnitValue.isInfinite) EXPIRATION_NONE.toLong else readUnsignedLong(buffer)
         }
      }
      version match {
         case ver if Constants.isVersionPre22(ver) =>
            val lifespan = readDuration(usingDefaultLifespan)
            val maxIdle = readDuration(usingDefaultMaxIdle)
            (new ExpirationParam(lifespan, TimeUnitValue.SECONDS), new ExpirationParam(maxIdle, TimeUnitValue.SECONDS))
         case _ => // from 2.2 onwards
            val timeUnits = TimeUnitValue.decodePair(buffer.readByte())
            val lifespanDuration = readDurationIfNeeded(timeUnits._1)
            val maxIdleDuration = readDurationIfNeeded(timeUnits._2)
            (new ExpirationParam(lifespanDuration, timeUnits._1), new ExpirationParam(maxIdleDuration, timeUnits._2))
      }
   }

   override def createSuccessResponse(header: HotRodHeader, prev: Array[Byte]): Response =
      createResponse(header, toResponse(header.op), Success, prev)

   override def createNotExecutedResponse(header: HotRodHeader, prev: Array[Byte]): Response =
      createResponse(header, toResponse(header.op), OperationNotExecuted, prev)

   override def createNotExistResponse(header: HotRodHeader): Response =
      createResponse(header, toResponse(header.op), KeyDoesNotExist, null)

   private def createResponse(h: HotRodHeader, op: OperationResponse, st: OperationStatus, prev: Array[Byte]): Response = {
      if (hasFlag(h, ForceReturnPreviousValue)) {
         val adjustedStatus = (h.op, st) match {
            case (NewHotRodOperation.PutRequest, Success) => SuccessWithPrevious
            case (NewHotRodOperation.PutIfAbsentRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case (NewHotRodOperation.ReplaceRequest, Success) => SuccessWithPrevious
            case (NewHotRodOperation.ReplaceIfUnmodifiedRequest, Success) => SuccessWithPrevious
            case (NewHotRodOperation.ReplaceIfUnmodifiedRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case (NewHotRodOperation.RemoveRequest, Success) => SuccessWithPrevious
            case (NewHotRodOperation.RemoveIfUnmodifiedRequest, Success) => SuccessWithPrevious
            case (NewHotRodOperation.RemoveIfUnmodifiedRequest, OperationNotExecuted) => NotExecutedWithPrevious
            case _ => st
         }

         adjustedStatus match {
            case SuccessWithPrevious | NotExecutedWithPrevious =>
               new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
                  h.clientIntel, op, adjustedStatus, h.topologyId, if (prev == null) None else Some(prev))
            case _ =>
               new Response(h.version, h.messageId, h.cacheName, h.clientIntel, op, adjustedStatus, h.topologyId)
         }

      }
      else
         new Response(h.version, h.messageId, h.cacheName, h.clientIntel, op, st, h.topologyId)
   }

   override def createGetResponse(h: HotRodHeader, entry: CacheEntry[Array[Byte], Array[Byte]]): Response = {
      val op = h.op
      if (entry != null && op == GetRequest)
         new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
            GetResponse, Success, h.topologyId,
            Some(entry.getValue))
      else if (entry != null && op == GetWithVersionRequest) {
         val version = entry.getMetadata.version().asInstanceOf[NumericVersion].getVersion
         new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
            h.clientIntel, GetWithVersionResponse, Success, h.topologyId,
            Some(entry.getValue), version)
      } else if (op == GetRequest)
         new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
            GetResponse, KeyDoesNotExist, h.topologyId, None)
      else
         new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
            h.clientIntel, GetWithVersionResponse, KeyDoesNotExist,
            h.topologyId, None, 0)
   }

   override def customReadHeader(h: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext,
                                 out: java.util.List[AnyRef]): Unit = {
      (h.op: @switch) match {
         case NewHotRodOperation.AuthRequest =>
            var authCtx = hrCtx.operationDecodeContext.asInstanceOf[AuthRequestContext]
            // first time read
            if (authCtx == null) {
               val mech = readMaybeString(buffer)
               mech.foreach(m => {
                  authCtx = new AuthRequestContext(m)
                  hrCtx.operationDecodeContext = authCtx
                  buffer.markReaderIndex()
               })
            }
            val clientResponse = readMaybeRangedBytes(buffer)
            clientResponse.foreach(cr => {
               authCtx.response = cr
               // If we were able to read everything add the context to the output
               out.add(hrCtx)
            })
         case NewHotRodOperation.ExecRequest =>
            var execCtx = hrCtx.operationDecodeContext.asInstanceOf[ExecRequestContext]
            // first time read
            if (execCtx == null) {
               val name = readMaybeString(buffer)
               name.foreach(n => {
                  execCtx = new ExecRequestContext(n)
                  hrCtx.operationDecodeContext = execCtx
                  buffer.markReaderIndex()
               })
            }

            if (execCtx != null) {
               var params = execCtx.params
               if (params == null) {
                  val paramCount = readMaybeVInt(buffer)
                  paramCount.foreach(p => {
                     params = new HashMap[String, Bytes](p)
                     execCtx.paramSize = p
                     execCtx.params = params
                     buffer.markReaderIndex()
                  })
               }
               @tailrec def addEntry(map: Map[String, Bytes]): Boolean = {
                  val complete = for {
                     key <- readMaybeString(buffer)
                     value <- readMaybeRangedBytes(buffer)
                  } yield {
                     map.put(key, value)
                     buffer.markReaderIndex()
                  }
                  if (complete.isDefined) {
                     // If we are the same size as param size we are done, otherwise continue until we
                     // can't read anymore or finally get to size
                     if (map.size() < execCtx.paramSize) {
                        addEntry(map)
                     } else true
                  } else false
               }
               if (addEntry(params)) {
                  // If we were able to read everything add the context to the output
                  out.add(hrCtx)
               }
            }
         case _ =>
            // This operation doesn't need additional reads - has everything to process
            out.add(hrCtx)
      }
   }

   override def customReadKey(h: HotRodHeader, buffer: ByteBuf, hrCtx: CacheDecodeContext,
                              out: java.util.List[AnyRef]): AnyRef = {
      (h.op: @switch) match {
         case NewHotRodOperation.BulkGetRequest =>
            val count = readUnsignedInt(buffer)
            if (isTrace) trace("About to create bulk response, count = %d", count)
            new BulkGetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               BulkGetResponse, Success, h.topologyId, count)
         case NewHotRodOperation.BulkGetKeysRequest =>
            val scope = readUnsignedInt(buffer)
            if (isTrace) trace("About to create bulk get keys response, scope = %d", scope)
            new BulkGetKeysResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, BulkGetKeysResponse, Success, h.topologyId, scope)
         case NewHotRodOperation.QueryRequest =>
            val query = readRangedBytes(buffer)
            val result = server.query(cache, query)
            new QueryResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               h.topologyId, result)
         case NewHotRodOperation.AddClientListenerRequest =>
            val listenerId = readRangedBytes(buffer)
            val includeState = buffer.readByte() == 1
            val filterFactoryInfo = readNamedFactory(buffer)
            val converterFactoryInfo = readNamedFactory(buffer)
            val useRawData = h.version match {
               case ver if Constants.isVersion2x(ver) => buffer.readByte() == 1
               case _ => false
            }
            val reg = server.getClientListenerRegistry
            reg.addClientListener(this, ch, h, listenerId, cache, includeState,
                  (filterFactoryInfo, converterFactoryInfo), useRawData)
            decoder.checkpointTo(HotRodDecoderState.DECODE_HEADER)
            null
         case NewHotRodOperation.RemoveClientListenerRequest =>
            val listenerId = readRangedBytes(buffer)
            val reg = server.getClientListenerRegistry
            val removed = reg.removeClientListener(listenerId, cache)
            if (removed)
               createSuccessResponse(h, null)
            else
               createNotExecutedResponse(h, null)
         case NewHotRodOperation.PutAllRequest | NewHotRodOperation.GetAllRequest =>
            decoder.checkpointTo(HotRodDecoderState.DECODE_PARAMETERS)
         case NewHotRodOperation.IterationStartRequest =>
            val segments = readOptRangedBytes(buffer)
            val namedFactory = if (Constants.isVersionPre24(h.version)) {
               for (factory <- readOptString(buffer)) yield (factory, List[Bytes]())
            } else {
               for (factory <- readOptString(buffer)) yield (factory, readOptionalParams(buffer))
            }
            val batchSize = readUnsignedInt(buffer)
            val metadata = Constants.isVersionPost24(h.version) && buffer.readByte() != 0
            val iterationId = server.iterationManager.start(cache.getName, segments.map(JavaBitSet.valueOf), namedFactory, batchSize, metadata)
            new IterationStartResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId, iterationId)
         case NewHotRodOperation.IterationNextRequest =>
            val iterationId = readString(buffer)
            val iterationResult = server.iterationManager.next(cache.getName, iterationId)
            new IterationNextResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId, iterationResult)
         case NewHotRodOperation.IterationEndRequest =>
            val iterationId = readString(buffer)
            val removed = server.iterationManager.close(cache.getName, iterationId)
            new Response(h.version, h.messageId, h.cacheName, h.clientIntel, IterationEndResponse, if (removed) Success else InvalidIteration, h.topologyId)
         case _ => null
      }
   }

   private def readNamedFactory(buffer: ByteBuf): NamedFactory = {
      for {
         factoryName <- readOptionalString(buffer)
      } yield (factoryName, readOptionalParams(buffer))
   }

   private def readOptionalString(buffer: ByteBuf): Option[String] = {
      val string = readString(buffer)
      if (string.isEmpty) None else Some(string)
   }

   private def readOptionalParams(buffer: ByteBuf): List[Bytes] = {
      val numParams = buffer.readByte()
      if (numParams > 0) {
         var params = ListBuffer[Bytes]()
         for (i <- 0 until numParams) params += readRangedBytes(buffer)
         params.toList
      } else List.empty
   }

   def getKeyMetadata(h: HotRodHeader, k: Array[Byte], cache: Cache): GetWithMetadataResponse = {
      val ce = cache.getCacheEntry(k)
      if (ce != null) {
         val ice = ce.asInstanceOf[InternalCacheEntry]
         val entryVersion = ice.getMetadata.version().asInstanceOf[NumericVersion]
         val v = ce.getValue
         val lifespan = if (ice.getLifespan < 0) -1 else (ice.getLifespan / 1000).toInt
         val maxIdle = if (ice.getMaxIdle < 0) -1 else (ice.getMaxIdle / 1000).toInt
         new GetWithMetadataResponse(h.version, h.messageId, h.cacheName,
            h.clientIntel, GetWithMetadataResponse, Success, h.topologyId,
            Some(v), entryVersion.getVersion, ice.getCreated, lifespan, ice.getLastUsed, maxIdle)
      } else {
         new GetWithMetadataResponse(h.version, h.messageId, h.cacheName,
            h.clientIntel, GetWithMetadataResponse, KeyDoesNotExist, h.topologyId,
            None, 0, -1, -1, -1, -1)
      }
   }

   override def customReadValue(decoder: HotRodDecoder, h: HotRodHeader,
       hrCtx: CacheDecodeContext, buffer: ByteBuf, cache: Cache): AnyRef = {
      (h.op: @switch) match {
         case NewHotRodOperation.PutAllRequest =>
            var map = hrCtx.putAllMap
            if (map == null) {
              map = new HashMap[Bytes, Bytes]
              hrCtx.putAllMap = map
            }
            for (i <- map.size until hrCtx.params.valueLength) {
              val k = readRangedBytes(buffer)
              val v = readRangedBytes(buffer)
              map.put(k, v)
              // We check point after each read entry
              decoder.checkpoint
            }
            cache.putAll(map, hrCtx.buildMetadata)
            new Response(h.version, h.messageId, h.cacheName, h.clientIntel,
               PutAllResponse, Success, h.topologyId)
         case NewHotRodOperation.GetAllRequest =>
           var set = hrCtx.getAllSet
           if (set == null) {
             set = new HashSet[Bytes]
             hrCtx.getAllSet = set
           }
           for (i <- set.size until hrCtx.params.valueLength) {
             val key = readRangedBytes(buffer)
             set.add(key)
             decoder.checkpoint
           }
           val results = cache.getAll(set)
           new GetAllResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               GetAllResponse, Success, h.topologyId, immutable.Map[Bytes, Bytes]() ++ results.asScala)
        case _ => null
     }
   }

   override def createStatsResponse(ctx: CacheDecodeContext, t: NettyTransport): StatsResponse = {
      val cacheStats = ctx.cache.getStats
      val stats = mutable.Map.empty[String, String]
      stats += ("timeSinceStart" -> cacheStats.getTimeSinceStart.toString)
      stats += ("currentNumberOfEntries" -> cacheStats.getCurrentNumberOfEntries.toString)
      stats += ("totalNumberOfEntries" -> cacheStats.getTotalNumberOfEntries.toString)
      stats += ("stores" -> cacheStats.getStores.toString)
      stats += ("retrievals" -> cacheStats.getRetrievals.toString)
      stats += ("hits" -> cacheStats.getHits.toString)
      stats += ("misses" -> cacheStats.getMisses.toString)
      stats += ("removeHits" -> cacheStats.getRemoveHits.toString)
      stats += ("removeMisses" -> cacheStats.getRemoveMisses.toString)
      stats += ("totalBytesRead" -> t.getTotalBytesRead)
      stats += ("totalBytesWritten" -> t.getTotalBytesWritten)


      val h = ctx.header
      if (!Constants.isVersionPre24(h.version)) {
         val registry = ctx.getCacheRegistry(h.cacheName)
         Option(registry.getComponent(classOf[ClusterCacheStats])).foreach(clusterCacheStats => {
            stats += ("globalCurrentNumberOfEntries" -> clusterCacheStats.getCurrentNumberOfEntries.toString)
            stats += ("globalStores" -> clusterCacheStats.getStores.toString)
            stats += ("globalRetrievals" -> clusterCacheStats.getRetrievals.toString)
            stats += ("globalHits" -> clusterCacheStats.getHits.toString)
            stats += ("globalMisses" -> clusterCacheStats.getMisses.toString)
            stats += ("globalRemoveHits" -> clusterCacheStats.getRemoveHits.toString)
            stats += ("globalRemoveMisses" -> clusterCacheStats.getRemoveMisses.toString)
         })
      }
      new StatsResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         immutable.Map[String, String]() ++ stats, h.topologyId)
   }

   override def createErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      t match {
         case _ : SuspectException => createNodeSuspectedErrorResponse(h, t)
         case e: IllegalLifecycleStateException => createIllegalLifecycleStateErrorResponse(h, t)
         case i: IOException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               ParseError, h.topologyId, i.toString)
         case t: TimeoutException =>
            new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               OperationTimedOut, h.topologyId, t.toString)
         case c: CacheException => c.getCause match {
            // JGroups and remote exceptions (inside RemoteException) can come wrapped up
            case _ : org.jgroups.SuspectedException => createNodeSuspectedErrorResponse(h, t)
            case _ : IllegalLifecycleStateException => createIllegalLifecycleStateErrorResponse(h, t)
            case _ => createServerErrorResponse(h, t)
         }
         case p: PrivilegedActionException => createErrorResponse(h, p.getCause)
         case t: Throwable => createServerErrorResponse(h, t)
      }
   }

   private def createNodeSuspectedErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         NodeSuspected, h.topologyId, t.toString)
   }

   private def createIllegalLifecycleStateErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         IllegalLifecycleState, h.topologyId, t.toString)
   }

   private def createServerErrorResponse(h: HotRodHeader, t: Throwable): ErrorResponse = {
      new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
         ServerError, h.topologyId, createErrorMsg(t))
   }

   def createErrorMsg(t: Throwable): String = {
      val causes = mutable.LinkedHashSet[Throwable]()
      var initial = t
      while (initial != null && !causes.contains(initial)) {
         causes += initial
         initial = initial.getCause
      }
      causes.mkString("\n")
   }

   override def getOptimizedCache(h: HotRodHeader, c: Cache, cacheCfg: Configuration): Cache = {
      val isTransactional = cacheCfg.transaction().transactionMode().isTransactional
      val isClustered = cacheCfg.clustering().cacheMode().isClustered

      var optCache = c
      h.op match {
         case op if h.op.isConditional() && isClustered && !isTransactional =>
            warnConditionalOperationNonTransactional(h.op.toString)
         case _ => // no-op
      }

      if (hasFlag(h, SkipCacheLoader)) {
         h.op match {
            case op if h.op.canSkipCacheLoading() =>
               optCache = optCache.withFlags(SKIP_CACHE_LOAD)
            case _ =>
         }
      }
      if (hasFlag(h, SkipIndexing)) {
         h.op match {
            case op if h.op.canSkipIndexing() =>
               optCache = optCache.withFlags(SKIP_INDEXING)
            case _ =>
         }
      }
      if (!hasFlag(h, ForceReturnPreviousValue)) {
         h.op match {
            case op if h.op.isNotConditionalAndCanReturnPrevious() =>
               optCache = optCache.withFlags(IGNORE_RETURN_VALUES)
            case _ =>
         }
      } else {
         h.op match {
            case op if h.op.canReturnPreviousValue() && !isTransactional =>
               warnForceReturnPreviousNonTransactional(h.op.toString)
            case _ => // no-op
         }
      }
      optCache
   }

   def normalizeAuthorizationId(id: String): String = {
      val realm = id.indexOf('@')
      if (realm >= 0) id.substring(0, realm) else id
   }

   /**
    * Convert an expiration value into milliseconds
    */
   override def toMillis(param: ExpirationParam, h: SuitableHeader): Long = {
      if (Constants.isVersionPre22(h.version)) super.toMillis(param, h)
      else
         if (param.duration > 0) {
            val javaTimeUnit = param.unit.toJavaTimeUnit(h)
            javaTimeUnit.toMillis(param.duration)
         } else {
            param.duration
         }
   }
}

class AuthRequestContext(mech: String) {
   var response: Bytes = _
}

class ExecRequestContext(name: String) {
   var paramSize: Int = _
   var params: Map[String, Bytes] = _
}
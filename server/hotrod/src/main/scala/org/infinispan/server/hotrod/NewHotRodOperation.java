package org.infinispan.server.hotrod;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 9.0
 */
public enum NewHotRodOperation {
   // Puts
   PutRequest(true, DecoderRequirements.VALUE),
   PutIfAbsentRequest(true, DecoderRequirements.VALUE),
   // Replace
   ReplaceRequest(true, DecoderRequirements.VALUE),
   ReplaceIfUnmodifiedRequest(true, DecoderRequirements.VALUE),
   // Contains
   ContainsKeyRequest(true, DecoderRequirements.KEY),
   // Gets
   GetRequest(true, DecoderRequirements.KEY),
   GetWithVersionRequest(true, DecoderRequirements.KEY),
   GetWithMetadataRequest(true, DecoderRequirements.KEY),
   // Removes
   RemoveRequest(true, DecoderRequirements.KEY),
   RemoveIfUnmodifiedRequest(true, DecoderRequirements.PARAMETERS),

   // Operation(s) that end after Header is read
   PingRequest(false, DecoderRequirements.HEADER),
   StatsRequest(false, DecoderRequirements.HEADER),
   ClearRequest(false, DecoderRequirements.HEADER),
   QuitRequest(false, DecoderRequirements.HEADER),
   SizeRequest(false, DecoderRequirements.HEADER),

   // Operation(s) that end after Custom Header is read
   AuthRequest(false, DecoderRequirements.HEADER_CUSTOM),
   ExecRequest(false, DecoderRequirements.HEADER_CUSTOM),

   // Operations that end after a Custom Key is read
   BulkGetRequest(false, DecoderRequirements.KEY_CUSTOM),
   BulkGetKeysRequest(false, DecoderRequirements.KEY_CUSTOM),
   QueryRequest(false, DecoderRequirements.KEY_CUSTOM),
   AddClientListenerRequest(false, DecoderRequirements.KEY_CUSTOM),
   RemoveClientListenerRequest(false, DecoderRequirements.KEY_CUSTOM),
   IterationStartRequest(false, DecoderRequirements.KEY_CUSTOM),
   IterationNextRequest(false, DecoderRequirements.KEY_CUSTOM),
   IterationEndRequest(false, DecoderRequirements.KEY_CUSTOM),

   AuthMechListRequest(false, DecoderRequirements.VALUE_CUSTOM),
   // Operations that end after a Custom Value is read
   PutAllRequest(false, DecoderRequirements.VALUE_CUSTOM),
   GetAllRequest(false, DecoderRequirements.VALUE_CUSTOM)
   ;

   private final boolean requiresSingleKey;
   private final DecoderRequirements decodeRequirements;

   NewHotRodOperation(boolean requiresSingleKey, DecoderRequirements decodeRequirements) {
      this.requiresSingleKey = requiresSingleKey;
      this.decodeRequirements = decodeRequirements;
   }

   DecoderRequirements getDecoderRequirements() {
      return decodeRequirements;
   }

   boolean requireSingleKey() {
      return requiresSingleKey;
   }

   boolean canSkipIndexing() {
      switch (this) {
         case PutRequest:
         case RemoveRequest:
         case PutIfAbsentRequest:
         case RemoveIfUnmodifiedRequest:
         case ReplaceRequest:
         case ReplaceIfUnmodifiedRequest:
         case PutAllRequest:
            return true;
         default:
            return false;
      }
   }

   boolean canSkipCacheLoading() {
      switch (this) {
         case PutRequest:
         case GetRequest:
         case GetWithVersionRequest:
         case RemoveRequest:
         case ContainsKeyRequest:
         case BulkGetRequest:
         case GetWithMetadataRequest:
         case BulkGetKeysRequest:
         case PutAllRequest:
            return true;
         default:
            return false;
      }
   }

   boolean isNotConditionalAndCanReturnPrevious() {
      return this == PutRequest;
   }

   boolean canReturnPreviousValue() {
      switch (this) {
         case PutRequest:
         case RemoveRequest:
         case PutIfAbsentRequest:
         case ReplaceRequest:
         case ReplaceIfUnmodifiedRequest:
         case RemoveIfUnmodifiedRequest:
            return true;
         default:
            return false;
      }
   }

   boolean isConditional() {
      switch (this) {
         case PutIfAbsentRequest:
         case ReplaceRequest:
         case ReplaceIfUnmodifiedRequest:
         case RemoveIfUnmodifiedRequest:
            return true;
         default:
            return false;
      }
   }
}

enum DecoderRequirements {
   HEADER,
   HEADER_CUSTOM,
   KEY,
   KEY_CUSTOM,
   PARAMETERS,
   PARAMETERS_CUSTOM,
   VALUE,
   VALUE_CUSTOM
}

package org.infinispan.hotrod.exceptions;

/**
 * Base class for exceptions reported by the hot rod client.
 *
 * @since 14.0
 */
public class HotRodClientException extends RuntimeException {
   private long messageId = -1;
   private int errorStatusCode = -1;

   public HotRodClientException() {
   }

   public HotRodClientException(String message) {
      super(message);
   }

   public HotRodClientException(Throwable cause) {
      super(cause);
   }

   public HotRodClientException(String message, Throwable cause) {
      super(message, cause);
   }

   public HotRodClientException(String remoteMessage, long messageId, int errorStatusCode) {
      super(remoteMessage);
      this.messageId = messageId;
      this.errorStatusCode = errorStatusCode;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder(getClass().getName());
      sb.append(":");
      if (messageId != -1) sb.append("Request for messageId=").append(messageId);
      if (errorStatusCode != -1) sb.append(" returned ").append(toErrorMsg(errorStatusCode));
      String message = getLocalizedMessage();
      if (message != null) sb.append(": ").append(message);
      return sb.toString();
   }

   private String toErrorMsg(int errorStatusCode) {
      return String.format("server error (status=0x%x)", errorStatusCode);
   }

   public boolean isServerError() {
      return errorStatusCode != -1;
   }
}

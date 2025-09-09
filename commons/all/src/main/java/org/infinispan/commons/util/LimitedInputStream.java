package org.infinispan.commons.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link InputStream} that limits the number of bytes that can be read from an underlying stream.
 * When the limit is reached, this stream will behave as if the end of the stream has been reached.
 *
 * @author William Burns
 * @since 16.0
 */
public class LimitedInputStream extends InputStream {
   private final InputStream underlying;
   private long limit;

   /**
    * Creates a new LimitedInputStream that wraps the provided InputStream and limits the number of bytes that can be
    * read from it.
    * @param underlying the InputStream to wrap.
    * @param limit the maximum number of bytes to read.
    */
   public LimitedInputStream(InputStream underlying, long limit) {
      if (underlying == null) {
         throw new NullPointerException("Underlying InputStream cannot be null");
      }
      if (limit < 0) {
         throw new IllegalArgumentException("Limit cannot be negative");
      }
      this.underlying = underlying;
      this.limit = limit;
   }

   @Override
   public int read() throws IOException {
      if (limit <= 0) {
         return -1;
      }
      int b = underlying.read();
      if (b != -1) {
         limit--;
      }
      return b;
   }

   @Override
   public int read(byte[] b, int off, int len) throws IOException {
      if (limit <= 0) {
         return -1;
      }
      if (len > limit) {
         len = (int) limit;
      }
      int bytesRead = underlying.read(b, off, len);
      if (bytesRead != -1) {
         limit -= bytesRead;
      }
      return bytesRead;
   }

   @Override
   public long skip(long n) throws IOException {
      if (n <= 0) {
         return 0;
      }
      if (n > limit) {
         n = limit;
      }
      long bytesSkipped = underlying.skip(n);
      if (bytesSkipped > 0) {
         limit -= bytesSkipped;
      }
      return bytesSkipped;
   }

   @Override
   public int available() throws IOException {
      int available = underlying.available();
      if (available > limit) {
         return (int) limit;
      }
      return available;
   }

   @Override
   public void close() throws IOException {
      underlying.close();
   }

   @Override
   public synchronized void mark(int readlimit) {
      // Mark/reset not supported
   }

   @Override
   public synchronized void reset() throws IOException {
      throw new IOException("mark/reset not supported");
   }

   @Override
   public boolean markSupported() {
      return false;
   }
}

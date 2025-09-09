package org.infinispan.remoting.transport.jgroups;

import org.jgroups.util.BaseDataOutputStream;

/**
 * An output stream that writes to a specific portion of a byte array.
 *
 * @author Bela Ban
 * @since 5.0
 */
public class JGroupsPartialOutputStream extends BaseDataOutputStream {
    private final byte[] buf;
    private final int offset;
    private final int length;
    private int pos;

    public JGroupsPartialOutputStream(byte[] buf, int offset, int length, int pos) {
        this.buf = buf;
        this.offset = offset;
        this.length = length;
        this.pos = pos;
    }

    @Override
    public void write(int b) {
        if (pos >= offset && pos < offset + length) {
            buf[pos] = (byte) b;
        }
        pos++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int start_copy_pos_stream = Math.max(pos, offset);
        int end_copy_pos_stream = Math.min(pos + len, offset + length);
        int bytes_to_copy = end_copy_pos_stream - start_copy_pos_stream;

        if (bytes_to_copy > 0) {
            int srcPos = off + (start_copy_pos_stream - pos);
            int destPos = start_copy_pos_stream - offset;
            System.arraycopy(b, srcPos, buf, destPos, bytes_to_copy);
        }
        pos += len;
    }

    public int position() {
        return pos;
    }

   @Override
   protected void ensureCapacity(int bytes) {
      // Ignore as we don't resize
   }
}

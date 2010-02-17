package org.jvnet.hudson;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Kohsuke Kawaguchi
 */
public class BlobTreeReader extends BlobTreeBase {
    public BlobTreeReader(File content) {
        super(content);
    }

    private static enum Policy {
        MATCH, FLOOR, CEIL
    }

    /**
     * Locates the blob that has the specified tag and starts reading it sequentially.
     */
    public Blob readBlob(long tag) throws IOException {
        return readBlob(tag, Policy.MATCH);
    }

    public Blob readBlobCeil(long tag) throws IOException {
        return readBlob(tag, Policy.CEIL);
    }

    public Blob readBlobFloor(long tag) throws IOException {
        return readBlob(tag, Policy.FLOOR);
    }

    public Blob readBlob(long tag, Policy policy) throws IOException {
        final byte[] back = new byte[32*sizeOfBackPtr];

        final RandomAccessFile idx = new RandomAccessFile(index,"r");

        class HeaderBlock {
            long pos = -1;
            final byte[] buf = new byte[12];

            void readAt(long pos) throws IOException {
                this.pos = pos;
                idx.seek(pos);
                idx.readFully(buf);
            }
            int seq() {
                int s = intAt(buf, 0);
                assert s>0;
                return s;
            }
            long tag() {
                return longAt(buf,4);
            }
            int height() {
                return BlobTree.height(seq());
            }

            /**
             * Reads a BLOB that this block points to.
             */
            Blob blob() throws IOException {
                idx.seek(pos-height()*sizeOfBackPtr-sizeOfLong);
                long blobPos = idx.readLong();
                RandomAccessFile in = new RandomAccessFile(content,"r");
                in.seek(blobPos);
                byte[] buf = new byte[in.readInt()];
                in.readFully(buf);
                return new Blob(tag(),buf);
            }
        }
        HeaderBlock header = new HeaderBlock();

        lock.readLock().lock();
        try {
            // start at the last entry
            long len = idx.length();
            if (len==0)     return null;    // no record written yet
            header.readAt(len - 12/*header size*/);

            OUTER:
            while (true) {
                // read the index of BLOB at pos
                final long t = header.tag();

                if (t<tag) {
                    if (policy== Policy.FLOOR)
                        return header.blob();
                    return null; // no match found
                }
                if (t==tag) {
                    // found the match
                    return header.blob();
                } else {
                    // continue searching

                    // read and follow back pointers.
                    int h = header.height();
                    idx.seek(header.pos-h*sizeOfBackPtr);
                    idx.readFully(back,0,h*sizeOfBackPtr);
                    for (int i=h-1; i>=0; i--) {
                        // find the biggest leap we can make
                        long pt = longAt(back,i*sizeOfBackPtr);
                        assert pt<t;
                        if (pt>=tag) {
                            header.readAt(longAt(back,i*sizeOfBackPtr+sizeOfLong));
                            assert pt==header.tag();
                            continue OUTER;
                        }
                    }

                    // there's no exact match, and prev and header are the two nodes that surround the specified tag.
                    switch (policy) {
                    case MATCH:     return null;
                    case FLOOR:
                        header.readAt(longAt(back,sizeOfLong));
                        return header.blob();
                    case CEIL:      return header.blob();
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
            idx.close();
        }
    }
}

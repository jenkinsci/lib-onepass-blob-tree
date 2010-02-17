package org.jvnet.hudson;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Kohsuke Kawaguchi
 */
public class BlobTreeReader extends BlobTreeBase implements Closeable {
    /**
     * Refers to the index file.
     */
    final RandomAccessFile idx;
    /**
     * Refers to the payload file that stores blobs.
     */
    final RandomAccessFile body;

    public BlobTreeReader(File content) throws IOException {
        super(content);
        idx = new RandomAccessFile(index,"r");
        body = new RandomAccessFile(content,"r");
    }

    class Cursor {
        /**
         * Byte offset of the record in the index file that this cursor is pointing at.
         */
        long pos = -1;
        /**
         * The header portion of the index file, excluding back pointers.
         */
        final byte[] buf = new byte[12];

        /**
         * Moves the cursor to the specified byte position in the index.
         */
        void seek(long pos) throws IOException {
            this.pos = pos;
            idx.seek(pos);
            idx.readFully(buf);
        }

        /**
         * Obtains the seq # of the current BLOB that this cursor points to.
         */
        int seq() {
            int s = intAt(buf, 0);
            assert s>0;
            return s;
        }

        /**
         * Obtains the tag # of the current BLOB that this cursor points to.
         */
        long tag() {
            return longAt(buf,4);
        }

        /**
         * How many back pointers does the current index record have?
         */
        int height() {
            return BlobTreeBase.height(seq());
        }

        /**
         * Reads a BLOB that this cursor points to.
         */
        Blob blob() throws IOException {
            idx.seek(pos-height()*sizeOfBackPtr-sizeOfLong);
            long blobPos = idx.readLong();
            body.seek(blobPos);
            byte[] buf = new byte[body.readInt()];
            body.readFully(buf);
            return new Blob(tag(),buf);
        }

        /**
         * Moves the cursor to the next blob.
         *
         * @return
         *      false if this is the last record and there's no next entry
         */
        boolean next() throws IOException {
            long newPos = pos + sizeOfLong * 2 + sizeOfInt + sizeOfBackPtr * BlobTreeBase.height(seq() + 1);
            if (newPos > idx.length())  return false;
            seek(newPos);
            return true;
        }
    }

    private static enum Policy {
        MATCH, FLOOR, CEIL
    }

    /**
     * Positions the cursor to the blob that matches the given tag (and the policy),
     * or null if no such blob was found. This is the gut of the search logic.
     */
    private Cursor seek(long tag, Policy policy) throws IOException {
        final byte[] back = new byte[32*sizeOfBackPtr];

        Cursor cur = new Cursor();

        // start at the last entry
        long len = idx.length();
        if (len==0)     return null;    // no record written yet
        cur.seek(len - 12/*header size*/);

        OUTER:
        while (true) {
            // read the index of BLOB at pos
            final long t = cur.tag();

            if (t<tag) {
                if (policy== Policy.FLOOR)
                    return cur;
                return null; // no match found
            }
            if (t==tag) {
                // found the match
                return cur;
            } else {
                // continue searching

                // read and follow back pointers.
                int h = cur.height();
                idx.seek(cur.pos-h*sizeOfBackPtr);
                idx.readFully(back,0,h*sizeOfBackPtr);
                for (int i=h-1; i>=0; i--) {
                    // find the biggest leap we can make
                    long pt = longAt(back,i*sizeOfBackPtr);
                    assert pt<t;
                    if (pt>=tag) {
                        cur.seek(longAt(back,i*sizeOfBackPtr+sizeOfLong));
                        assert pt== cur.tag();
                        continue OUTER;
                    }
                }

                // there's no exact match, and prev and header are the two nodes that surround the specified tag.
                switch (policy) {
                case MATCH:     return null;
                case FLOOR:
                    cur.seek(longAt(back,sizeOfLong));
                    return cur;
                case CEIL:      return cur;
                }
            }
        }
    }

    /**
     * Locates the blob that has the specified tag and starts reading it sequentially.
     */
    public Blob at(long tag) throws IOException {
        return read(tag, Policy.MATCH);
    }

    public Blob ceil(long tag) throws IOException {
        return read(tag, Policy.CEIL);
    }

    public Blob floor(long tag) throws IOException {
        return read(tag, Policy.FLOOR);
    }

    synchronized Blob read(long tag, Policy policy) throws IOException {
        lock.readLock().lock();
        try {
            Cursor cur = seek(tag,policy);
            return cur!=null ? cur.blob() : null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Reads all blogs whose tag is within the [start,end) range.
     *
     * @return
     *      can be empty but never null.
     */
    public synchronized List<Blob> range(long start, long end) throws IOException {
        lock.readLock().lock();
        try {
            Cursor cur = seek(start, Policy.CEIL);
            if (cur==null || end<=cur.tag())    return Collections.emptyList(); // no match

            List<Blob> r = new ArrayList<Blob>();
            while (cur.tag()<end) {
                r.add(cur.blob());
                if (!cur.next())    break;
            }
            return r;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void close() throws IOException {
        idx.close();
        body.close();
    }
}

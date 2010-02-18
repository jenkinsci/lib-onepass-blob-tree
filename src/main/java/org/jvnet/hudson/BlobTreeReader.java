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
        final byte[] buf = new byte[sizeOfIndexEntry];

        /**
         * Moves the cursor to the specified byte position in the index.
         */
        void seek(long pos) throws IOException {
            this.pos = pos;
            idx.seek(pos);
            idx.readFully(buf);
        }

        /**
         * Obtains the tag # of the current BLOB that this cursor points to.
         */
        long tag() {
            return longAt(buf,0);
        }

        /**
         * Reads a BLOB that this cursor points to.
         */
        Blob blob() throws IOException {
            long blobPos = longAt(buf,sizeOfLong);
            body.seek(blobPos);
            byte[] buf = new byte[body.readInt()];
            body.readFully(buf);
            return new Blob(tag(),buf);
        }

        /**
         * Moves the cursor to the next blob.
         *
         */
        Cursor next() throws IOException {
            long newPos = pos + sizeOfIndexEntry;
            if (newPos >= idx.length())  return null;
            seek(newPos);
            return this;
        }

        Cursor prev() throws IOException {
            long newPos = pos - sizeOfIndexEntry;
            if (newPos<0)  return null;
            seek(newPos);
            return this;
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
        // [s,e) designates the range of binary search
        long s=0;
        long e=idx.length()/sizeOfIndexEntry;

        final long total = e;

        Cursor cur = new Cursor();
        while (s<e) {
            long middle = (s+e)/2;
            cur.seek(middle*sizeOfIndexEntry);
            long pivot = cur.tag();
            if (pivot==tag)
                return cur; // found a match
            if (pivot<tag) {
                s = middle+1;
            } else {
                e = middle;
            }
        }

        // no exact match
        assert s==e;

        switch (policy) {
        case FLOOR:
            if (s==0)   return null;
            cur.seek((s-1)*sizeOfIndexEntry);
            return cur;
        case CEIL:
            if (s==total)   return null;
            cur.seek(s*sizeOfIndexEntry);
            return cur;
        default:        return null;
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
            while (cur!=null && cur.tag()<end) {
                r.add(cur.blob());
                cur = cur.next();
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

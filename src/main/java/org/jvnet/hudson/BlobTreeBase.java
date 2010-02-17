package org.jvnet.hudson;

import java.io.File;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 *
 * <h2>Index File Structure</h2>
 * <pre>
 * struct IndexFile {
 *   Record[]
 * }
 *
 * struct Record {
 *      // the corresponding BLOB record starts at this offset in the content file.
 *      long payloadOffset;
 *      // back pointers that point to earlier Records.
 *      // N = {@linkplain #height(int) height}(seq)
 *      BackPointer[N] backpointers;
 *
 *      // sequential number of this record. 1 origin.
 *      // <b>Backpointer.offset refers to this position in the record</b>
 *      int seq;
 *      // tag of the BLOB
 *      long tag;
 * }
 *
 * struct BackPointer {
 *      // together this says "there's a BLOB that has the specified tag, and its index starts
 *      // at the specified offset of the index file. 
 *      long tag;
 *      long offset;
 * }
 * </pre>
 *
 * @author Kohsuke Kawaguchi
 */
abstract class BlobTreeBase {
    final File index;
    final File content;

    final String lockKey;
    /**
     * Lock that controls the contention between {@link BlobTreeReader} and {@link BlobTreeWriter}.
     */
    final ReadWriteLock lock;


    public BlobTreeBase(File content) {

        index = new File(content.getPath()+".index");
        this.content = content;

        lockKey = ("\u0000"+content.getPath()).intern(); // make a unique String instance unlikely to be used by anyone else
        synchronized (LOCKS) {
            ReentrantReadWriteLock lock = LOCKS.get(lockKey);
            if (lock==null)
                LOCKS.put(lockKey,lock = new ReentrantReadWriteLock());
            this.lock = lock;
        }
    }

    /**
     * This computes the number of backpointers we need for the given sequence #.
     */
    static int height(int seq) {
        assert seq>0;

        int r = 1;
        while((seq%2)==0) {
            r++;
            seq/=2;
        }

        // if we are the first node of this height, then the top-most back pointer will
        // always points to NIL, so there's no point in writing that entry
        if (seq==1)   r--;

        return r;
    }

    static int updateHeight(int seq) {
        assert seq>0;

        int r = 1;
        while((seq%2)==0) {
            r++;
            seq/=2;
        }
        return r;
    }

    static long longAt(byte[] buf, int pos) {
        return ((long)(intAt(buf,pos)) << 32) + (intAt(buf,pos+4) & 0xFFFFFFFFL);
    }

    static int intAt(byte[] buf, int pos) {
        int a = byteAt(buf,pos  );
        int b = byteAt(buf,pos+1);
        int c = byteAt(buf,pos+2);
        int d = byteAt(buf,pos+3);

        return ((a << 24) + (b << 16) + (c << 8) + d);
    }

    static int byteAt(byte[] buf, int pos) {
        return ((int)buf[pos])&0xFF;
    }

    static final int sizeOfInt = 4;
    static final int sizeOfLong = 8;
    static final int sizeOfBackPtr = sizeOfLong + sizeOfLong; /* one for offset, the other for tag*/

    private static final WeakHashMap<String, ReentrantReadWriteLock> LOCKS = new WeakHashMap<String, ReentrantReadWriteLock>();
}

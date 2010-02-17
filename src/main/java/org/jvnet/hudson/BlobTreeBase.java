package org.jvnet.hudson;

import java.io.File;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Kohsuke Kawaguchi
 */
abstract class BlobTreeBase {
    final File index;
    final File content;

    final String lockKey;
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

    static final int sizeOfLong = 8;
    static final int sizeOfBackPtr = sizeOfLong + sizeOfLong; /* one for offset, the other for tag*/

    private static final WeakHashMap<String, ReentrantReadWriteLock> LOCKS = new WeakHashMap<String, ReentrantReadWriteLock>();
}

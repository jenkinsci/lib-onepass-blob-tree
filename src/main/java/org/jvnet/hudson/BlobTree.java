package org.jvnet.hudson;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Kohsuke Kawaguchi
 */
public class BlobTree implements Closeable {
    private final File index;
    private final File content;

    private final CountingDataOutputStream iout;
    private final CountingDataOutputStream cout;

    /**
     * Sequential blob number to be written next, which determines the height.
     *
     * 0 acts as a sentry, and on disk it starts from 1. 
     */
    private int seq = 0;

    /**
     * Tag numbers need to be monotonic.
     */
    private long lastTag = 0;

    private final long[] back = new long[32];

    private BlobWriterStream blob = new BlobWriterStream();

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public BlobTree(File content) throws FileNotFoundException {
        index = new File(content.getPath()+".index");
        this.content = content;

        this.iout = new CountingDataOutputStream(new FileOutputStream(index));
        this.cout = new CountingDataOutputStream(new FileOutputStream(content));
    }

    /**
     * Deletes the underlying files.
     */
    public void delete() throws IOException {
        iout.close();
        cout.close();
        index.delete();
        content.delete();
    }

    private class BlobWriterStream extends ByteArrayOutputStream {
        private boolean closed;
        private long tag;

        public void resetTo(long tag) {
            this.tag = tag;
            closed = false;
            super.reset();
        }

        @Override
        public void close() throws IOException {
            super.close();

            if (!closed) {
                closed = true;

                // update of the index needs to happen in sync with the read operation
                lock.writeLock().lock();
                try {
                    // pointer to the blob in content
                    iout.writeLong(cout.getCount());

                    // write back pointers
                    int h = height(seq);
                    for (int i=0; i<h; i++)
                        iout.writeLong(back[i]);

                    // update back pointers
                    long pos = iout.getCount();
                    int uh = updateHeight(seq);
                    for (int i=0; i< uh; i++)
                        back[i] = pos;

                    // BLOB number
                    iout.writeInt(seq);

                    // tag
                    iout.writeLong(tag);

                    cout.writeInt(blob.size());
                    writeTo(cout);
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }


    /**
     * Starts writing the next blob.
     */
    public OutputStream writeNext(long tag) throws IOException {
        if (seq>0) {
            if (tag<lastTag)
                    throw new IllegalArgumentException("Last written tag was "+lastTag+" and tried to write a smaller tag "+tag);
            blob.close();
        }
        seq++;

        lastTag = tag;
        blob.resetTo(tag);
        return blob;
    }

    /**
     * Finishes writing.
     */
    public void close() throws IOException {
        if (seq>1)
            blob.close();
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
        final byte[] back = new byte[32*8];

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
                idx.seek(pos-height()*8-8);
                long blobPos = idx.readLong();
                RandomAccessFile in = new RandomAccessFile(content,"r");
                in.seek(blobPos);
                byte[] buf = new byte[in.readInt()];
                in.readFully(buf);
                return new Blob(tag(),buf);
            }
        }
        HeaderBlock header = new HeaderBlock();
        HeaderBlock prev = new HeaderBlock();

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
                    idx.seek(header.pos-h*8);
                    idx.readFully(back,0,h*8);
                    for (int i=h-1; i>=0; i--) {
                        // find the biggest leap we can make
                        prev.readAt(longAt(back,i*8));
                        assert prev.tag()<t;
                        if (prev.tag()>=tag) {
                            // swap(&header,&prev)
                            HeaderBlock swap = header;
                            header = prev;
                            prev = swap;
                            continue OUTER;
                        }
                    }

                    // there's no exact match, and prev and header are the two nodes that surround the specified tag.
                    switch (policy) {
                    case MATCH:     return null;
                    case FLOOR:     return prev.blob();
                    case CEIL:      return header.blob();
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
            idx.close();
        }
    }


    /**
     * This computes the number of backpointers we need for the given sequence #.
     */
    private static int height(int seq) {
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

    private static int updateHeight(int seq) {
        assert seq>0;

        int r = 1;
        while((seq%2)==0) {
            r++;
            seq/=2;
        }
        return r;
    }

    private static long longAt(byte[] buf, int pos) {
        return ((long)(intAt(buf,pos)) << 32) + (intAt(buf,pos+4) & 0xFFFFFFFFL);
    }

    private static int intAt(byte[] buf, int pos) {
        int a = byteAt(buf,pos  );
        int b = byteAt(buf,pos+1);
        int c = byteAt(buf,pos+2);
        int d = byteAt(buf,pos+3);
        
        return ((a << 24) + (b << 16) + (c << 8) + d);
    }
    
    private static int byteAt(byte[] buf, int pos) {
        return ((int)buf[pos])&0xFF;
    }
}

package org.jvnet.hudson;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Kohsuke Kawaguchi
 */
public class BlobTreeWriter extends BlobTreeBase implements Closeable {
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
    private final long[] backTag = new long[32];

    private BlobWriterStream blob = new BlobWriterStream();

    public BlobTreeWriter(File content) throws FileNotFoundException {
        super(content);

        this.iout = new CountingDataOutputStream(new FileOutputStream(index));
        this.cout = new CountingDataOutputStream(new FileOutputStream(content));
    }

    /**
     * Deletes the underlying files.
     */
    public void delete() throws IOException {
        close();
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
                    for (int i=0; i<h; i++) {
                        iout.writeLong(backTag[i]);
                        iout.writeLong(back[i]);
                    }

                    // update back pointers
                    long pos = iout.getCount();
                    int uh = updateHeight(seq);
                    for (int i=0; i< uh; i++) {
                        back[i] = pos;
                        backTag[i] = tag;
                    }

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
     * Completes writing.
     */
    public void close() throws IOException {
        if (seq>0)
            blob.close();
        iout.close();
        cout.close();
    }



}

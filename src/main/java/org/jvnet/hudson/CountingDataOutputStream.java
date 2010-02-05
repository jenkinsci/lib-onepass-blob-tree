package org.jvnet.hudson;

import org.apache.commons.io.output.CountingOutputStream;

import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 * {@link DataOutputStream} that can also count the total number of bytes written.
 *
 * @author Kohsuke Kawaguchi
 */
final class CountingDataOutputStream extends DataOutputStream {
    private final CountingOutputStream counter;

    CountingDataOutputStream(OutputStream out) {
        super(new CountingOutputStream(out));
        this.counter = (CountingOutputStream)super.out;
    }

    public long getCount() {
        return counter.getByteCount();
    }
}

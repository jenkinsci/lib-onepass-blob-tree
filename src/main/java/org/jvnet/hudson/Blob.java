package org.jvnet.hudson;

/**
 * @author Kohsuke Kawaguchi
 */
public class Blob {
    public final long tag;
    public final byte[] payload;

    public Blob(long tag, byte[] payload) {
        this.tag = tag;
        this.payload = payload;
    }
}

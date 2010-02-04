package org.jvnet.hudson;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Kohsuke Kawaguchi
 */
public class BlobTreeTest extends TestCase {
    public void test1() throws Exception {
        BlobTree t = new BlobTree(new File("test"));
        write(t, 1, "one");
        write(t, 3, "three");
        write(t, 5, "five");
        write(t, 7, "seven");
        write(t, 9, "nine");
        write(t, 11, "eleven");

        Blob b = t.readBlob(3);
        assertEquals(3,b.tag);
        assertEquals("three",new String(b.payload));
    }

    private void write(BlobTree t, int tag, String content) throws IOException {
        OutputStream out = t.writeNext(tag);
        out.write(content.getBytes());
    }
}

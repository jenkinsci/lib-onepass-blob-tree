package org.jvnet.hudson;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static java.lang.Long.*;

/**
 * @author Kohsuke Kawaguchi
 */
public class BlobTreeTest extends TestCase {
    BlobTree t;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        t  = new BlobTree(new File("test"));
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        t.delete();
    }

    /**
     * Make sure we assert on incorrect tag order.
     */
    public void testOrderCheck() throws IOException {
        t.writeNext(MIN_VALUE); // should allow us to write LONG_MIN
        t.writeNext(MAX_VALUE);
        try {
            t.writeNext(0);
            fail();
        } catch (IllegalArgumentException e) {
            // as expected
        }
    }

    public void testEmptyRead() throws IOException {
        assertNull(t.readBlob(0));
        assertNull(t.readBlobFloor(MAX_VALUE));
        assertNull(t.readBlobCeil(MIN_VALUE));
    }

    /**
     * Basic full read/write test.
     */
    public void test1() throws Exception {
        // create sparse tags and variable size payloads
        Map<Integer,String> source = new TreeMap<Integer, String>();
        StringBuilder payload = new StringBuilder();
        for (int i=1; i<=19; i+=2) {
            payload.append("x");
            source.put(i,payload.toString());
        }

        for (Entry<Integer,String> e : source.entrySet()) {
            write(t, e.getKey(), e.getValue());
        }
        t.close();

        t.readBlobCeil(0);

        // test exact match
        for (Entry<Integer,String> e : source.entrySet()) {
            Integer tag = e.getKey();
            String p = e.getValue();
            
            assertBlob(t.readBlob(tag), tag, p);
            assertNull(t.readBlob(tag+1));

            assertBlob(t.readBlobFloor(tag+1), tag, p);
            assertBlob(t.readBlobCeil(tag-1), tag, p);
        }
    }

    /**
     * A BLOB that's partially written shouldn't be visible to the reader.
     */
    public void testPartialWrite() throws Exception {
        OutputStream o = t.writeNext(1);

        // since 1 is still being written, this should fail to read
        assertNull(t.readBlob(1));
        assertNull(t.readBlobCeil(0));
        assertNull(t.readBlobFloor(2));

        o.write(5);
        o.close();
        assertNotNull(t.readBlob(1));
        assertNotNull(t.readBlobCeil(0));
        assertNotNull(t.readBlobFloor(2));
    }

    private void assertBlob(Blob b, int tag, String payload) {
        assertNotNull("Expected tag "+tag,b);
        assertEquals(tag,b.tag);
        assertEquals(payload,new String(b.payload));
    }

    private void write(BlobTree t, int tag, String content) throws IOException {
        OutputStream out = t.writeNext(tag);
        out.write(content.getBytes());
    }
}

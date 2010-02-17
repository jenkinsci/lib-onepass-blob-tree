package org.jvnet.hudson;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static java.lang.Long.*;

/**
 * @author Kohsuke Kawaguchi
 */
public class BlobTreeTest extends TestCase {
    BlobTreeWriter w;
    BlobTreeReader r;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        w = new BlobTreeWriter(new File("test"));
        r = new BlobTreeReader(new File("test"));
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        w.delete();
    }

    /**
     * Make sure we assert on incorrect tag order.
     */
    public void testOrderCheck() throws IOException {
        w.writeNext(MIN_VALUE); // should allow us to write LONG_MIN
        w.writeNext(MAX_VALUE);
        try {
            w.writeNext(0);
            fail();
        } catch (IllegalArgumentException e) {
            // as expected
        }
    }

    public void testEmptyRead() throws IOException {
        assertNull(r.at(0));
        assertNull(r.floor(MAX_VALUE));
        assertNull(r.ceil(MIN_VALUE));
    }

    /**
     * Basic full read/write test.
     */
    public void test1() throws Exception {
        Map<Integer, String> source = createTestDataSet();

        r.ceil(0);

        // test exact match
        for (Entry<Integer,String> e : source.entrySet()) {
            Integer tag = e.getKey();
            String p = e.getValue();
            
            assertBlob(r.at(tag), tag, p);
            assertNull(r.at(tag+1));

            assertBlob(r.floor(tag+1), tag, p);
            assertBlob(r.ceil(tag-1), tag, p);
        }
    }

    /**
     * Test range read.
     */
    public void testRangeRead() throws Exception {
        Map<Integer, String> source = createTestDataSet();

        assertEquals(0,r.range(0,1).size());
        assertEquals(0,r.range(99,Integer.MAX_VALUE).size());

        List<Blob> l = r.range(0, 3);
        assertEquals(1,l.size());
        assertBlob(l.get(0),1,source.get(1));

        l = r.range(3,6);
        assertEquals(2,l.size());
        assertBlob(l.get(0),3,source.get(3));
        assertBlob(l.get(1),5,source.get(5));
    }

    private Map<Integer, String> createTestDataSet() throws IOException {
        // create sparse tags and variable size payloads
        Map<Integer,String> source = new TreeMap<Integer, String>();
        StringBuilder payload = new StringBuilder();
        for (int i=1; i<=19; i+=2) {
            payload.append("x");
            source.put(i,payload.toString());
        }

        for (Entry<Integer,String> e : source.entrySet()) {
            write(w, e.getKey(), e.getValue());
        }
        w.close();
        return source;
    }

    /**
     * A BLOB that's partially written shouldn't be visible to the reader.
     */
    public void testPartialWrite() throws Exception {
        OutputStream o = w.writeNext(1);

        // since 1 is still being written, this should fail to read
        assertNull(r.at(1));
        assertNull(r.ceil(0));
        assertNull(r.floor(2));

        o.write(5);
        o.close();
        assertNotNull(r.at(1));
        assertNotNull(r.ceil(0));
        assertNotNull(r.floor(2));
    }

    private void assertBlob(Blob b, int tag, String payload) {
        assertNotNull("Expected tag "+tag,b);
        assertEquals(tag,b.tag);
        assertEquals(payload,new String(b.payload));
    }

    private void write(BlobTreeWriter t, int tag, String content) throws IOException {
        OutputStream out = t.writeNext(tag);
        out.write(content.getBytes());
    }
}

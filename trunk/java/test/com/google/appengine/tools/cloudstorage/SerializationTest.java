package com.google.appengine.tools.cloudstorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Test to make sure the ReadableByteChannels returned by the library serialize and deserialize in a
 * working state.
 *
 */
@RunWith(JUnit4.class)
public class SerializationTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig(), new LocalFileServiceTestConfig(),
      new LocalBlobstoreServiceTestConfig(), new LocalDatastoreServiceTestConfig());


  private enum TestFile {
    SMALL(new GcsFilename("unit-tests", "smallFile"), 100), MEDIUM(new GcsFilename(
        "unit-tests", "mediumFile"), 2000);

    public final GcsFilename filename;
    public final int contentSize;

    TestFile(GcsFilename filename, int contentSize) {
      this.filename = filename;
      this.contentSize = contentSize;
    }
  }

  @Before
  public void setup() {
    helper.setUp();
  }

  private void createFiles(GcsService gcsService) throws IOException {
    Charset utf8 = Charset.forName("UTF-8");
    for (TestFile file : TestFile.values()) {
      StringBuffer contents = new StringBuffer(file.contentSize);
      for (int i = 0; i < file.contentSize; i++) {
        contents.append(i % 10);
      }
      GcsOutputChannel outputChannel =
          gcsService.createOrReplace(file.filename, GcsFileOptions.builder().withDefaults());
      try{
        outputChannel.write(utf8.encode(CharBuffer.wrap(contents.toString())));
      } finally {
        outputChannel.close();
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testSimpleGcsInputChannelLocal() throws IOException, ClassNotFoundException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    createFiles(gcsService);
    ReadableByteChannel readChannel = gcsService.openReadChannel(TestFile.SMALL.filename, 0);
    try {
      ByteBuffer buffer = ByteBuffer.allocate(5);
      readChannel.read(buffer);
      buffer.rewind();
      assertTrue(Arrays.equals(new byte[] {'0', '1', '2', '3', '4'}, buffer.array()));
      buffer.rewind();
      readChannel = reconstruct(readChannel);
      readChannel.read(buffer);
      buffer.rewind();
      assertTrue(Arrays.equals(new byte[] {'5', '6', '7', '8', '9'}, buffer.array()));
    } finally {
      readChannel.close();
    }
  }


  @Test
  public void testOauthSerializes() throws IOException, ClassNotFoundException {
    RawGcsService rawGcsService = OauthRawGcsServiceFactory.createOauthRawGcsService();
    GcsService gcsService = new GcsServiceImpl(rawGcsService, new RetryParams());
    ReadableByteChannel readChannel = gcsService.openReadChannel(TestFile.SMALL.filename, 0);
    ReadableByteChannel reconstruct = reconstruct(readChannel);
    readChannel.close();
    reconstruct.close();
  }

  @Test
  public void testPrefetchingGcsInputChannelLocal() throws IOException, ClassNotFoundException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    createFiles(gcsService);
    @SuppressWarnings("resource")
    ReadableByteChannel readChannel =
        gcsService.openPrefetchingReadChannel(TestFile.MEDIUM.filename, 0, 1031);
    ByteBuffer buffer = ByteBuffer.allocate(5);
    int read = readChannel.read(buffer);
    int count = 0;
    while (read != -1) {
      buffer.rewind();
      assertTrue(Arrays.equals(new byte[] {'0', '1', '2', '3', '4'}, buffer.array()));
      buffer.rewind();
      readChannel = reconstruct(readChannel);
      read = readChannel.read(buffer);
      buffer.rewind();
      assertTrue(Arrays.equals(new byte[] {'5', '6', '7', '8', '9'}, buffer.array()));
      read = readChannel.read(buffer);
      count++;
    }
    buffer.rewind();
    readChannel = reconstruct(readChannel);
    read = readChannel.read(buffer);
    assertEquals(-1, read);
    assertTrue(Arrays.equals(new byte[] {'5', '6', '7', '8', '9'}, buffer.array()));
    assertEquals(TestFile.MEDIUM.contentSize / 10, count);
    readChannel.close();
  }

  private ReadableByteChannel reconstruct(ReadableByteChannel readChannel)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    try {
      oout.writeObject(readChannel);
    } finally {
      oout.close();
    }
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()));
    return (ReadableByteChannel) in.readObject();
  }

}

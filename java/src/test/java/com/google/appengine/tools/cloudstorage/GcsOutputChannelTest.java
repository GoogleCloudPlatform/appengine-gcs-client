/*
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.cloudstorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * Test the OutputChannels (writing to GCS)
 */
@RunWith(Parameterized.class)
public class GcsOutputChannelTest {

  private static final int BUFFER_SIZE = 2 * 1024 * 1024;
  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig(), new LocalBlobstoreServiceTestConfig(),
      new LocalDatastoreServiceTestConfig());

  private final boolean reconstruct;


  public GcsOutputChannelTest(boolean reconstruct) {
    this.reconstruct = reconstruct;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  @Before
  public void setUp() throws Exception {
    helper.setUp();
  }

  @After
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  /**
   * Writes a file with the content supplied by pattern repeated over and over until the desired
   * size is reached. After each write call reconstruct is called the output channel.
   *
   *  We don't want to call close on the output channel while writing because this will prevent
   * additional writes. Similarly we don't put the close in a finally block, because we don't want
   * the partly written data to be used in the event of an exception.
   */
  @SuppressWarnings("resource")
  public void writeFile(String name, int size, byte[]... patterns)
      throws IOException, ClassNotFoundException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    GcsFilename filename = new GcsFilename("GcsOutputChannelTestBucket", name);
    GcsOutputChannelImpl outputChannel = (GcsOutputChannelImpl) gcsService.createOrReplace(
        filename, GcsFileOptions.getDefaultInstance());
    outputChannel = reconstruct(outputChannel);
    assertEquals(0, outputChannel.buf.capacity());
    int written = 0;
    while (written < size) {
      for (byte[] pattern : patterns) {
        int toWrite = Math.min(pattern.length, size - written);
        if (toWrite > 0) {
          outputChannel.write(ByteBuffer.wrap(pattern, 0, toWrite));
          assertTrue("Unexpected buffer size: " + outputChannel.buf,
              outputChannel.buf.capacity() <= outputChannel.getBufferSizeBytes());
          written += toWrite;
        }
      }
      if (reconstruct) {
        outputChannel.waitForOutstandingWrites();
        int remaining = outputChannel.buf.remaining();
        outputChannel = reconstruct(outputChannel);
        if (remaining == 0) {
          assertEquals(0, outputChannel.buf.capacity());
        } else {
          assertEquals(outputChannel.getBufferSizeBytes(), outputChannel.buf.capacity());
          assertEquals(remaining, outputChannel.buf.remaining());
        }
      }
    }
    outputChannel.close();
    assertNull(outputChannel.buf);
    outputChannel = reconstruct(outputChannel);
    assertNull(outputChannel.buf);
    assertFalse(outputChannel.isOpen());
    outputChannel = reconstruct(outputChannel);
  }

  /**
   * Read the file and verify it contains the expected pattern the expected number of times.
   */
  private void verifyContent(String name, int expectedSize, byte[]... contents) throws IOException {
    @SuppressWarnings("resource")
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    for (byte[] content : contents) {
      bout.write(content);
    }
    bout.close();
    byte[] content = bout.toByteArray();
    GcsService gcsService = GcsServiceFactory.createGcsService();
    GcsFilename filename = new GcsFilename("GcsOutputChannelTestBucket", name);
    try (GcsInputChannel readChannel =
        gcsService.openPrefetchingReadChannel(filename, 0, BUFFER_SIZE)) {
      ByteBuffer result = ByteBuffer.allocate(content.length);
      ByteBuffer wrapped = ByteBuffer.wrap(content);
      int size = 0;
      int read = readFully(readChannel, result);
      while (read != -1) {
        assertTrue(read > 0);
        size += read;
        result.rewind();
        result.limit(read);
        wrapped.limit(read);
        if (!wrapped.equals(result)) {
          assertEquals(wrapped, result);
        }
        read = readFully(readChannel, result);
      }
      assertEquals(expectedSize, size);
    }
  }

  private int readFully(GcsInputChannel readChannel, ByteBuffer result) throws IOException {
    int totalRead = 0;
    while (result.hasRemaining()) {
      int read = readChannel.read(result);
      if (read == -1) {
        if (totalRead == 0) {
          totalRead = -1;
        }
        break;
      } else {
        totalRead += read;
      }
    }
    return totalRead;
  }

  /**
   * Serializes and deserializes the the GcsOutputChannel. This simulates the writing of the file
   * continuing from a different request.
   */
  @SuppressWarnings("unchecked")
  private static <T> T reconstruct(T writeChannel)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bout = writeChannelToStream(writeChannel);
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()))) {
      return (T) in.readObject();
    }
  }

  private static ByteArrayOutputStream writeChannelToStream(Object value) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(value);
    }
    return bout;
  }

  @Test
  public void testSettingBufferSize() throws IOException {
    RawGcsService raw = GcsServiceFactory.createRawGcsService(ImmutableMap.<String, String>of());
    GcsFilename filename = new GcsFilename("GcsOutputChannelTestBucket", "testSettingBufferSize");
    GcsFileOptions fileOptions = GcsFileOptions.getDefaultInstance();
    int chunkSizeBytes = raw.getChunkSizeBytes();

    GcsService out = GcsServiceFactory.createGcsService(
        new GcsServiceOptions.Builder().setDefaultWriteBufferSize(null).build());
    assertEquals(BUFFER_SIZE, out.createOrReplace(filename, fileOptions).getBufferSizeBytes());

    out = GcsServiceFactory.createGcsService(
        new GcsServiceOptions.Builder().setDefaultWriteBufferSize(0).build());
    assertEquals(chunkSizeBytes, out.createOrReplace(filename, fileOptions).getBufferSizeBytes());

    out = GcsServiceFactory.createGcsService(
        new GcsServiceOptions.Builder().setDefaultWriteBufferSize(chunkSizeBytes).build());
    assertEquals(chunkSizeBytes, out.createOrReplace(filename, fileOptions).getBufferSizeBytes());

    out = GcsServiceFactory.createGcsService(
        new GcsServiceOptions.Builder().setDefaultWriteBufferSize(chunkSizeBytes + 1).build());
    assertEquals(chunkSizeBytes, out.createOrReplace(filename, fileOptions).getBufferSizeBytes());

    out = GcsServiceFactory.createGcsService(
        new GcsServiceOptions.Builder().setDefaultWriteBufferSize(chunkSizeBytes * 2).build());
    assertEquals(chunkSizeBytes * 2,
        out.createOrReplace(filename, fileOptions).getBufferSizeBytes());

    out = GcsServiceFactory.createGcsService(
        new GcsServiceOptions.Builder().setDefaultWriteBufferSize(Integer.MAX_VALUE).build());
    assertEquals((raw.getMaxWriteSizeByte() / chunkSizeBytes) * chunkSizeBytes,
        out.createOrReplace(filename, fileOptions).getBufferSizeBytes());
  }

  @Test
  public void testSingleLargeWrite() throws IOException, ClassNotFoundException {
    int size = 5 * BUFFER_SIZE;
    byte[] content = new byte[size];
    Random r = new Random();
    r.nextBytes(content);
    writeFile("SingleLargeWrite", size, content);
    verifyContent("SingleLargeWrite", size, content);
  }

  @Test
  public void testSmallWrites() throws IOException, ClassNotFoundException {
    byte[] content = new byte[100];
    Random r = new Random();
    r.nextBytes(content);
    int size = 27 * 1024;
    assertTrue(size < BUFFER_SIZE);
    writeFile("testSmallWrites", size, content);
    verifyContent("testSmallWrites", size, content);
  }

  /**
   * Tests writing in multiple segments that is > RawGcsService.getChunkSizeBytes() but less than
   * the buffer size in {@link GcsOutputChannelImpl}.
   */
  @Test
  public void testLargeWrites() throws IOException, ClassNotFoundException {
    byte[] content = new byte[(int) (BUFFER_SIZE * 0.477)];
    Random r = new Random();
    r.nextBytes(content);
    int size = (int) (2.5 * BUFFER_SIZE);
    writeFile("testLargeWrites", size, content);
    verifyContent("testLargeWrites", size, content);
  }

  @Test
  public void testWithRandomWriteSize() throws IOException, ClassNotFoundException {
    Random r = new Random();
    int writesNum = 30 + r.nextInt(30);
    byte[][] bytes = new byte[writesNum][];
    int size = 0;
    for (int i = 0; i < writesNum; i++) {
      byte[] temp;
      int type = r.nextInt(10);
      if (type < 6) {
        temp = new byte[1 + r.nextInt(2_000_000)];
      } else if (type < 8) {
        temp = new byte[2_000_000 + r.nextInt(3_000_000)];
      } else {
        temp = new byte[5_000_000 + r.nextInt(15_000_000)];
      }
      r.nextBytes(temp);
      bytes[i] = temp;
      size += temp.length;
    }
    writeFile("testRandomSize", size, bytes);
    verifyContent("testRandomSize", size, bytes);
  }

  @Test
  public void testUnalignedWrites() throws IOException, ClassNotFoundException {
    byte[] content = new byte[997];
    Random r = new Random();
    r.nextBytes(content);
    int size = 2377 * 1033;
    assertTrue(size > BUFFER_SIZE);
    writeFile("testUnalignedWrites", size, content);
    verifyContent("testUnalignedWrites", size, content);
  }

  @Test
  public void testAlignedWrites() throws IOException, ClassNotFoundException {
    byte[] content = new byte[BUFFER_SIZE];
    Random r = new Random();
    r.nextBytes(content);
    writeFile("testUnalignedWrites", 5 * BUFFER_SIZE, content);
    verifyContent("testUnalignedWrites", 5 * BUFFER_SIZE, content);
  }

  @Test
  public void testPartialFlush() throws IOException {
    byte[] content = new byte[BUFFER_SIZE - 1];
    Random r = new Random();
    r.nextBytes(content);

    GcsService gcsService = GcsServiceFactory.createGcsService();
    GcsFilename filename = new GcsFilename("GcsOutputChannelTestBucket", "testPartialFlush");
    try (GcsOutputChannel outputChannel =
        gcsService.createOrReplace(filename, GcsFileOptions.getDefaultInstance())) {
      outputChannel.write(ByteBuffer.wrap(content, 0, content.length));
      try (ByteArrayOutputStream bout = writeChannelToStream(outputChannel)) {
        assertTrue(bout.size() >= BUFFER_SIZE);
        outputChannel.waitForOutstandingWrites();
      }
      try (ByteArrayOutputStream bout = writeChannelToStream(outputChannel)) {
        assertTrue(bout.size() < BUFFER_SIZE);
        assertTrue(bout.size() > 0);
      }
    }
    verifyContent("testPartialFlush", BUFFER_SIZE - 1, content);
  }

  /**
   * The other tests in this file assume a buffer size of 2mb. If this is changed this test will
   * fail. Before fixing it update the other tests.
   * @throws IOException
   */
  @Test
  public void testBufferSize() throws IOException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    GcsFilename filename = new GcsFilename("GcsOutputChannelTestBucket", "testBufferSize");
    @SuppressWarnings("resource")
    GcsOutputChannel outputChannel =
        gcsService.createOrReplace(filename, GcsFileOptions.getDefaultInstance());
    assertEquals(BUFFER_SIZE, outputChannel.getBufferSizeBytes());
  }
}

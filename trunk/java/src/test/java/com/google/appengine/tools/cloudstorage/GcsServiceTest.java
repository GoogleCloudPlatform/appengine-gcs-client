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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * End to end test to test the basics of the GcsService. This class uses the in-process
 * implementation, but provides coverage for everything above the RawGcsService
 *
 */
@RunWith(JUnit4.class)
public class GcsServiceTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig(), new LocalFileServiceTestConfig(),
      new LocalBlobstoreServiceTestConfig(), new LocalDatastoreServiceTestConfig());
  private GcsService gcsService;
  private GcsFileOptions options;
  private List<GcsFilename> toDelete = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    helper.setUp();
    gcsService = GcsServiceFactory.createGcsService();
    options = GcsFileOptions.getDefaultInstance();
  }

  @After
  public void tearDown() throws Exception {
    for (GcsFilename filename : toDelete) {
      gcsService.delete(filename);
    }

    helper.tearDown();
  }

  @Test
  public void testCreateWithBuffer() throws IOException {
    String content = "FooBar";
    GcsFilename filename = new GcsFilename("testCreateWithBuffer", "testCreateWithBuffer");
    createFile(filename, content, false);
    try (GcsInputChannel readChannel = gcsService.openReadChannel(filename, 0)) {
      ByteBuffer result = ByteBuffer.allocate(7);
      int read = readChannel.read(result);
      result.limit(read);
      assertEquals(6, read);
      result.rewind();
      assertEquals(content, UTF_8.decode(result).toString());
    }
    gcsService.delete(filename);
  }

  @Test
  public void testReadWrittenData() throws IOException {
    String content = "FooBar";
    GcsFilename filename = new GcsFilename("testReadWrittenDataBucket", "testReadWrittenDataFile");
    createFile(filename, content, false);
    try (GcsOutputChannel outputChannel = gcsService.createOrReplace(filename, options)) {
      outputChannel.write(UTF_8.encode(CharBuffer.wrap(content)));
    }

    try (GcsInputChannel readChannel = gcsService.openReadChannel(filename, 0)) {
      ByteBuffer result = ByteBuffer.allocate(7);
      int read = readChannel.read(result);
      result.limit(read);
      assertEquals(6, read);
      result.rewind();
      assertEquals(content, UTF_8.decode(result).toString());
    }
  }

  @Test
  public void testWrite10mb() throws IOException {
    int length = 10 * 1024 * 1024 + 1;
    GcsFilename filename = new GcsFilename("testWrite10mbBucket", "testWrite10mbFile");
    byte[] content = createFile(filename, length, true);
    try (GcsInputChannel readChannel = gcsService.openReadChannel(filename, 0)) {
      verifyContent(content, readChannel, 25000);
    }
  }

  @Test
  public void testShortFileLongBuffer() throws IOException {
    int length = 1024;
    GcsFilename filename =
        new GcsFilename("testShortFileLongBufferBucket", "testShortFileLongBufferFile");
    byte[] content = createFile(filename, length, true);
    try (GcsInputChannel readChannel =
        gcsService.openPrefetchingReadChannel(filename, 0, 512 * 1024)) {
      verifyContent(content, readChannel, 13);
    }
  }

  @Test
  public void testDelete() throws IOException {
    int length = 1024;
    GcsFilename filename = new GcsFilename("testDeleteBucket", "testDeleteFile");
    assertFalse(gcsService.delete(filename));
    createFile(filename, length, true);
    assertTrue(gcsService.delete(filename));
    ByteBuffer result = ByteBuffer.allocate(1);
    try (GcsInputChannel readChannel = gcsService.openReadChannel(filename, 0)) {
      try {
        readChannel.read(result);
        fail();
      } catch (IOException e) {
      }
    }

    try (GcsInputChannel readChannel =
        gcsService.openPrefetchingReadChannel(filename, 0, 512 * 1024)) {
      try {
        readChannel.read(result);
        fail();
      } catch (IOException e) {
      }
    }
  }

  @Test
  public void testCompose() throws IOException {
    int length1 = 130;
    GcsFilename filename1 = new GcsFilename("testCompose", "file1");
    byte[] content1 = createFile(filename1, length1, false);
    int length2 = 2000;
    GcsFilename filename2 = new GcsFilename("testCompose", "file2");
    byte[] content2 = createFile(filename2, length2, true);
    GcsFilename composed = new GcsFilename("testCompose", "composed");
    gcsService.compose(
        ImmutableList.of(filename1.getObjectName(), filename2.getObjectName()), composed);
    byte[] composedContent = new byte[length1 + length2];
    System.arraycopy(content1, 0, composedContent, 0, length1);
    System.arraycopy(content2, 0, composedContent, length1, length2);
    try (GcsInputChannel channel = gcsService.openPrefetchingReadChannel(composed, 0, 512 * 1024)) {
      verifyContent(composedContent, channel, 13);
    }
    gcsService.delete(composed);
  }

  @Test
  public void testCopy() throws IOException {
    GcsFilename source = new GcsFilename("testCompose", "file1");
    byte[] content = createFile(source, 280, false);
    GcsFilename dest = new GcsFilename("testCopy", "file2");
    gcsService.copy(source, dest);
    try (GcsInputChannel channel = gcsService.openPrefetchingReadChannel(source, 0, 512 * 1024)) {
      verifyContent(content, channel, 13);
    }
    gcsService.delete(dest);
  }

  @Test
  public void testBufferedReads() throws IOException {
    int length = 1 * 1024 * 1024 + 1;
    GcsFilename filename = new GcsFilename("testBufferedReadsBucket", "testBufferedReadsFile");
    byte[] content = createFile(filename, length, true);
    try (GcsInputChannel readChannel =
        gcsService.openPrefetchingReadChannel(filename, 0, 512 * 1024)) {
      verifyContent(content, readChannel, 13);
    }
  }

  @Test
  public void testReadMetadata() throws IOException {
    Date start = new Date(System.currentTimeMillis() - 1000);
    int length = 1 * 1024 * 1024 + 1;
    GcsFilename filename = new GcsFilename("testReadMetadataBucket", "testReadMetadataFile");
    byte[] content = createFile(filename, length, false);
    GcsFileMetadata metadata = gcsService.getMetadata(filename);
    assertNotNull(metadata);
    assertEquals(filename, metadata.getFilename());
    assertEquals(content.length, metadata.getLength());
    assertEquals(options, metadata.getOptions());
    assertNotNull(metadata.getLastModified());
    assertTrue(metadata.getLastModified().before(new Date()));
    assertTrue(metadata.getLastModified().after(start));
  }

  @Test
  public void testEmptyMetadata() throws IOException {
    GcsFilename filename = new GcsFilename("testEmptyMetadataBucket", "testEmptyMetadataFile");
    GcsFileMetadata metadata = gcsService.getMetadata(filename);
    assertNull(metadata);
  }

  private byte[] createFile(GcsFilename filename, int length, boolean stream) throws IOException {
    byte[] content = new byte[length];
    Random r = new Random();
    r.nextBytes(content);
    createFile(filename, content, stream);
    return content;
  }

  private void createFile(GcsFilename filename, String content, boolean stream)
      throws IOException {
    createFile(filename, content.getBytes(UTF_8), stream);
  }

  private void createFile(GcsFilename filename, byte[] bytes, boolean stream) throws IOException {
    ByteBuffer src = ByteBuffer.wrap(bytes);
    if (stream) {
      try (GcsOutputChannel outputChannel = gcsService.createOrReplace(filename, options)) {
        outputChannel.write(src);
      }
    } else {
      gcsService.createOrReplace(filename, options, src);
    }
    toDelete.add(filename);
  }

  private void verifyContent(byte[] content, GcsInputChannel readChannel, int readSize)
      throws IOException {
    ByteBuffer result = ByteBuffer.allocate(readSize);
    ByteBuffer wrapped = ByteBuffer.wrap(content);
    int offset = 0;
    int read = readChannel.read(result);
    assertTrue(read != -1);
    while (read != -1) {
      result.limit(read);
      wrapped.position(offset);
      offset += read;
      wrapped.limit(offset);

      assertTrue(read > 0);
      assertTrue(read <= readSize);

      result.rewind();
      try {
        assertEquals(wrapped, result);
      } catch (AssertionError er) {
        er.printStackTrace();
        throw er;
      }
      read = readChannel.read(result);
    }
  }
}

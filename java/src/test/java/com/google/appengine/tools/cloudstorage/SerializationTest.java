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
import static org.junit.Assert.assertTrue;

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.common.collect.ImmutableSet;

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
import java.util.Arrays;

/**
 * Test to make sure the @{link GcsInputChannel}s returned by the library serialize and deserialize
 * in a working state.
 *
 */
@RunWith(JUnit4.class)
public class SerializationTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig(), new LocalBlobstoreServiceTestConfig(),
      new LocalDatastoreServiceTestConfig());

  private enum TestFile {
    SMALL(new GcsFilename("unit-tests", "smallFile"), 100),
    MEDIUM(new GcsFilename("unit-tests", "mediumFile"), 4000),
    LARGE(new GcsFilename("unit-tests", "largeFile"), 1024 * 1024 * 2);

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
    for (TestFile file : TestFile.values()) {
      StringBuffer contents = new StringBuffer(file.contentSize);
      for (int i = 0; i < file.contentSize; i++) {
        contents.append(i % 10);
      }
      try (GcsOutputChannel outputChannel =
          gcsService.createOrReplace(file.filename, GcsFileOptions.getDefaultInstance())) {
        outputChannel.write(UTF_8.encode(CharBuffer.wrap(contents.toString())));
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testSerializeClosedOutputChannel() throws IOException, ClassNotFoundException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    @SuppressWarnings("resource")
    GcsOutputChannel ch =
        gcsService.createOrReplace(TestFile.SMALL.filename, GcsFileOptions.getDefaultInstance());
    ch.close();
    reconstruct(ch);
  }

  @SuppressWarnings("resource")
  @Test
  public void testSimpleGcsInputChannelLocal() throws IOException, ClassNotFoundException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    createFiles(gcsService);
    GcsInputChannel readChannel = gcsService.openReadChannel(TestFile.SMALL.filename, 0);
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


  @SuppressWarnings("resource")
  @Test
  public void testOauthSerializes() throws IOException, ClassNotFoundException {
    RawGcsService rawGcsService =
        OauthRawGcsServiceFactory.createOauthRawGcsService(ImmutableSet.<HTTPHeader>of());
    GcsService gcsService = new GcsServiceImpl(rawGcsService, GcsServiceOptions.DEFAULT);
    GcsInputChannel readChannel = gcsService.openReadChannel(TestFile.SMALL.filename, 0);
    GcsInputChannel reconstruct = reconstruct(readChannel);
    readChannel.close();
    reconstruct.close();
  }

  @SuppressWarnings("resource")
  @Test
  public void testPrefetchingGcsInputChannelLocal() throws IOException, ClassNotFoundException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    createFiles(gcsService);
    GcsInputChannel readChannel =
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

  @SuppressWarnings("resource")
  @Test
  public void testPrefetchingExactRead() throws IOException, ClassNotFoundException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    createFiles(gcsService);
    GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(
        TestFile.MEDIUM.filename, 0, TestFile.MEDIUM.contentSize / 2);
    ByteBuffer buffer = ByteBuffer.allocate(10);
    int read = 0;
    for (int i = 0; i < TestFile.MEDIUM.contentSize / 10; i++) {
      readChannel = reconstruct(readChannel);
      read = readChannel.read(buffer);
      buffer.rewind();
      assertTrue(Arrays.equals(
          new byte[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}, buffer.array()));
      buffer.rewind();
    }
    readChannel = reconstruct(readChannel);
    read = readChannel.read(buffer);
    assertEquals(-1, read);
    readChannel.close();
  }

  @SuppressWarnings("resource")
  @Test
  public void testSerializingPrefetchOfLargeFileAtEof() throws IOException, ClassNotFoundException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    createFiles(gcsService);
    GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(
        TestFile.LARGE.filename, 0, TestFile.LARGE.contentSize / 2);
    ByteBuffer buffer = ByteBuffer.allocate(TestFile.LARGE.contentSize * 2);
    int read = readChannel.read(buffer);
    assertEquals(read, TestFile.LARGE.contentSize / 2);
    assertEquals(buffer.position(), TestFile.LARGE.contentSize / 2);
    read = readChannel.read(buffer);
    assertEquals(read, TestFile.LARGE.contentSize / 2);
    assertEquals(buffer.position(), TestFile.LARGE.contentSize);
    readChannel = reconstruct(readChannel);
    read = readChannel.read(buffer);
    assertEquals(-1, read);
    readChannel.close();
  }

  @SuppressWarnings("unchecked")
  private static <T> T reconstruct(T value)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(value);
    }
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()));
    T answer = (T) in.readObject();
    assertEquals(-1, in.read());
    return answer;
  }
}

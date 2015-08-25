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

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.common.base.Strings;
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
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

/**
 * End to end test to test the basics of the GcsService. This class uses the in-process
 * implementation, but provides coverage for everything above the RawGcsService
 *
 */
@RunWith(JUnit4.class)
public class GcsServiceTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig(), new LocalBlobstoreServiceTestConfig(),
      new LocalDatastoreServiceTestConfig());
  private GcsService gcsService;
  private final List<GcsFilename> toDelete = new ArrayList<>();
  private GcsFileOptions options;

  @Before
  public void setUp() throws Exception {
    helper.setUp();
    gcsService = GcsServiceFactory.createGcsService();
    options = new GcsFileOptions.Builder()
        .mimeType("text/json")
        .acl("public")
        .addUserMetadata("bla-name", "bla-value")
        .build();
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
  public void testWriteReadWithNamespaceChange() throws IOException {
    NamespaceManager.set("ns1");
    GcsFilename source = new GcsFilename("testCompose", "file1");
    byte[] content = createFile(source, 280, false);
    GcsFilename dest = new GcsFilename("testCopy", "file2");
    NamespaceManager.set("ns2");
    gcsService.copy(source, dest);
    NamespaceManager.set("ns3");
    try (GcsInputChannel channel = gcsService.openPrefetchingReadChannel(source, 0, 512 * 1024)) {
      verifyContent(content, channel, 13);
    }
    NamespaceManager.set("");
    gcsService.delete(dest);
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
    assertEquals(options, gcsService.getMetadata(dest).getOptions());
    gcsService.delete(dest);
  }

  @Test
  public void testUpdate() throws IOException {
    GcsFilename file = new GcsFilename("testUpdate", "file1");
    byte[] content = createFile(file, 280, false);
    GcsFileOptions options = gcsService.getMetadata(file).getOptions();
    assertEquals("text/json", options.getMimeType());
    options = new GcsFileOptions.Builder().mimeType("bla").build();
    gcsService.update(file, options);
    assertEquals("bla", options.getMimeType());
    try (GcsInputChannel channel = gcsService.openPrefetchingReadChannel(file, 0, 512 * 1024)) {
      verifyContent(content, channel, 13);
    }
    gcsService.delete(file);
  }

  @Test
  public void testList() throws IOException {
    String[] prefixes = {"", "/", "f", "dir1", "dir1/", "dir1/f", "dir1/dir3", "dir1/dir3/"};

    NavigableMap<String, ListItem> items = setupForListTests(true);
    ListResult result = gcsService.list("testList", null);
    verifyListResult(items, null, false, result);
    for (String prefix : prefixes) {
      ListOptions options = new ListOptions.Builder().setPrefix(prefix).setRecursive(true).build();
      result = gcsService.list("testList", options);
      verifyListResult(items, prefix, false, result);
    }

    items = setupForListTests(true);
    for (String prefix : prefixes) {
      ListOptions options = new ListOptions.Builder().setPrefix(prefix).setRecursive(false).build();
      result = gcsService.list("testList", options);
      verifyListResult(items, prefix, true, result);
    }
  }

  private void verifyListResult(NavigableMap<String, ListItem> items, String prefix,
      boolean recursive, ListResult result) {
    prefix = Strings.nullToEmpty(prefix);
    if (!prefix.isEmpty()) {
      String endPrefix = prefix.substring(0, prefix.length() - 1)
          + (char) (prefix.charAt(prefix.length() - 1) + 1);
      items = items.subMap(prefix, true, endPrefix, false);
    }
    if (recursive) {
      TreeMap<String, ListItem> temp = new TreeMap<>();
      for (ListItem item : items.values()) {
        String name = item.getName();
        int idx = name.indexOf('/', prefix.length());
        if (idx != -1) {
          name = name.substring(0, idx + 1);
          item = new ListItem.Builder().setName(name).setDirectory(true).build();
        }
        temp.put(name, item);
      }
      items = temp;
    } else {
      items = new TreeMap<>(items);
    }
    while (result.hasNext()) {
      ListItem item = result.next();
      ListItem expected = items.remove(item.getName());
      assertEquals(expected, item);
    }
    assertTrue(items.isEmpty());
  }

  private NavigableMap<String, ListItem> setupForListTests(boolean recursive) throws IOException {
    NavigableMap<String, ListItem> items = new TreeMap<>();
    String[] filenames = { "foo", "bla/file", "file", "file1", "file2", "s", "s1",
        "dir1/", "dir1/dir2/dir_2_2/", "dir1/dir3/", "dir1/dir3/file"};
    for (String filename : filenames) {
      ListItem item = createObject(filename, recursive);
      items.put(item.getName(), item);
    }
    for (int i = 1; i <= 100; i++) {
      ListItem item = createObject("dir1/f", i);
      items.put(item.getName(), item);
      if (i <= 10) {
        item = createObject("dir1/f" + i + "/", recursive);
        items.put(item.getName(), item);
      }
    }
    for (int i = 101; i <= 180; i++) {
      ListItem item = createObject("dir1/folder1/f", i);
      items.put(item.getName(), item);
    }
    for (int i = 181; i <= 190; i++) {
      ListItem item = createObject("dir1/folder2/f", i);
      items.put(item.getName(), item);
    }
    for (int i = 191; i <= 200; i++) {
      ListItem item = createObject("dir1/f" + i + "/", recursive);
      items.put(item.getName(), item);
    }
    return items;
  }

  private ListItem createObject(String name, boolean recursive) throws IOException {
    if (name.endsWith("/") && !recursive) {
      GcsFilename filename = new GcsFilename("testList", name);
      createFile(filename, 0, false);
      return new ListItem.Builder().setName(name).setDirectory(true).build();
    }
    int size = new Random().nextInt(100);
    GcsFilename filename = new GcsFilename("testList", name);
    createFile(filename, size, false);
    GcsFileMetadata metadata = gcsService.getMetadata(filename);
    return new ListItem.Builder().setName(name).setLength(size).setEtag(metadata.getEtag())
        .setLastModified(metadata.getLastModified()).build();
  }

  private ListItem createObject(String prefix, int idx) throws IOException {
    String name = prefix + idx;
    GcsFilename filename = new GcsFilename("testList", name);
    createFile(filename, idx, false);
    GcsFileMetadata metadata = gcsService.getMetadata(filename);
    return new ListItem.Builder().setName(name).setLength(idx).setEtag(metadata.getEtag())
        .setLastModified(metadata.getLastModified()).build();
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

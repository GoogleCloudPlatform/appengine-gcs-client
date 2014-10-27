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

package com.google.appengine.tools.cloudstorage.dev;

import static com.google.appengine.api.datastore.Entity.KEY_RESERVED_PROPERTY;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN_OR_EQUAL;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.ThreadManager;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileReadChannel;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileStat;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.GSFileOptions;
import com.google.appengine.api.files.GSFileOptions.GSFileOptionsBuilder;
import com.google.appengine.tools.cloudstorage.BadRangeException;
import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.ListItem;
import com.google.appengine.tools.cloudstorage.RawGcsService;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Environment;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@code RawGcsService} for dev_appserver. For now, uses datastore and
 * fileService so that the viewers can be re-used.
 */
@SuppressWarnings("deprecation")
final class LocalRawGcsService implements RawGcsService {

  static final int CHUNK_ALIGNMENT_BYTES = 256 * 1024;

  private static final DatastoreService DATASTORE = DatastoreServiceFactory.getDatastoreService();
  private static final FileService FILES = FileServiceFactory.getFileService();

  private static final String ENTITY_KIND_PREFIX = "_ah_FakeCloudStorage__";
  private static final String OPTIONS_PROP = "options";
  private static final String CREATION_TIME_PROP = "time";
  private static final String FILE_LENGTH_PROP = "length";

  static final class Token implements RawGcsCreationToken {
    private static final long serialVersionUID = 954846981243798905L;

    private final GcsFilename filename;
    private final GcsFileOptions options;
    private final long offset;
    private final AppEngineFile file;

    Token(GcsFilename filename, GcsFileOptions options, long offset, AppEngineFile file) {
      this.options = options;
      this.filename = checkNotNull(filename, "Null filename");
      this.offset = offset;
      this.file = checkNotNull(file, "Null file");
    }

    @Override
    public GcsFilename getFilename() {
      return filename;
    }

    @Override
    public long getOffset() {
      return offset;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + filename + ", " + offset + ")";
    }

    @Override
    public final boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Token other = (Token) o;
      return offset == other.offset && Objects.equals(filename, other.filename)
          && Objects.equals(options, other.options);
    }

    @Override
    public final int hashCode() {
      return Objects.hash(filename, offset, options);
    }
  }

  @Override
  public Token beginObjectCreation(GcsFilename filename, GcsFileOptions options, long timeoutMillis)
      throws IOException {
    return new Token(
        filename, options, 0, FILES.createNewGSFile(gcsOptsToGsOpts(filename, options)));
  }

  private GSFileOptions gcsOptsToGsOpts(GcsFilename filename, GcsFileOptions options) {
    GSFileOptionsBuilder builder = new GSFileOptionsBuilder();
    builder.setBucket(filename.getBucketName());
    builder.setKey(filename.getObjectName());
    if (options.getAcl() != null) {
      builder.setAcl(options.getAcl());
    }
    if (options.getCacheControl() != null) {
      builder.setCacheControl(options.getCacheControl());
    }
    if (options.getContentDisposition() != null) {
      builder.setContentDisposition(options.getContentDisposition());
    }
    if (options.getContentEncoding() != null) {
      builder.setContentEncoding(options.getContentEncoding());
    }
    if (options.getMimeType() != null) {
      builder.setMimeType(options.getMimeType());
    }
    for (Entry<String, String> entry : options.getUserMetadata().entrySet()) {
      builder.addUserMetadata(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  private Token append(RawGcsCreationToken token, ByteBuffer chunk) throws IOException {
    Token t = (Token) token;
    if (!chunk.hasRemaining()) {
      return t;
    }
    try (FileWriteChannel ch = FILES.openWriteChannel(t.file, false)) {
      int n = chunk.remaining();
      int r = ch.write(chunk);
      Preconditions.checkState(r == n, "%s: Bad write: %s != %s", this, r, n);
      return new Token(t.filename, t.options, t.offset + n, t.file);
    }
  }

  private static ScheduledThreadPoolExecutor writePool;
  static {
    try {
      writePool = new ScheduledThreadPoolExecutor(1, ThreadManager.backgroundThreadFactory());
    } catch (Exception e) {
      writePool = new ScheduledThreadPoolExecutor(1);
    }
  }

  /**
   * Runs calls in a background thread so that the results will actually be asynchronous.
   *
   * @see com.google.appengine.tools.cloudstorage.RawGcsService#continueObjectCreationAsync(
   *        com.google.appengine.tools.cloudstorage.RawGcsService.RawGcsCreationToken,
   *        java.nio.ByteBuffer, long)
   */
  @Override
  public Future<RawGcsCreationToken> continueObjectCreationAsync(final RawGcsCreationToken token,
      final ByteBuffer chunk, long timeoutMillis) {
    final Environment environment = ApiProxy.getCurrentEnvironment();
    return writePool.schedule(new Callable<RawGcsCreationToken>() {
      @Override
      public RawGcsCreationToken call() throws Exception {
        ApiProxy.setEnvironmentForCurrentThread(environment);
        return append(token, chunk);
      }
    }, 50, TimeUnit.MILLISECONDS);
  }

  private Key makeKey(GcsFilename filename) {
    return makeKey(filename.getBucketName(), filename.getObjectName());
  }

  private Key makeKey(String bucket, String object) {
    String origNamespace = NamespaceManager.get();
    try {
      NamespaceManager.set("");
      return KeyFactory.createKey(ENTITY_KIND_PREFIX + bucket, object);
    } finally {
      NamespaceManager.set(origNamespace);
    }
  }

  private Query makeQuery(String bucket) {
    String origNamespace = NamespaceManager.get();
    try {
      NamespaceManager.set("");
      return new Query(ENTITY_KIND_PREFIX + bucket);
    } finally {
      NamespaceManager.set(origNamespace);
    }
  }

  @Override
  public void finishObjectCreation(RawGcsCreationToken token, ByteBuffer chunk, long timeoutMillis)
      throws IOException {
    Token t = append(token, chunk);
    FILES.openWriteChannel(t.file, true).closeFinally();
    FileStat stats = FILES.stat(t.file);
    Entity e = new Entity(makeKey(t.filename));
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(t.options);
    }
    e.setUnindexedProperty(OPTIONS_PROP, new Blob(bout.toByteArray()));
    e.setUnindexedProperty(CREATION_TIME_PROP, System.currentTimeMillis());
    e.setUnindexedProperty(FILE_LENGTH_PROP, stats.getLength());
    DATASTORE.put(null, e);
  }

  private AppEngineFile nameToAppEngineFile(GcsFilename filename) {
    return new AppEngineFile(
        AppEngineFile.FileSystem.GS, filename.getBucketName() + "/" + filename.getObjectName());
  }

  @Override
  public GcsFileMetadata getObjectMetadata(GcsFilename filename, long timeoutMillis)
      throws IOException {
    Entity entity;
    try {
      entity = DATASTORE.get(null, makeKey(filename));
    } catch (EntityNotFoundException ex) {
      return null;
    }
    return createGcsFileMetadata(entity, filename);
  }

  private GcsFileMetadata createGcsFileMetadata(Entity entity, GcsFilename filename)
      throws IOException {
    GcsFileOptions options;
    try (ObjectInputStream in = new ObjectInputStream(
        new ByteArrayInputStream(((Blob) entity.getProperty(OPTIONS_PROP)).getBytes()))) {
      options = (GcsFileOptions) in.readObject();
    } catch (ClassNotFoundException e1) {
      throw new RuntimeException(e1);
    }
    Date creationTime = null;
    if (entity.getProperty(CREATION_TIME_PROP) != null) {
      creationTime = new Date((Long) entity.getProperty(CREATION_TIME_PROP));
    }
    long length;
    if (entity.getProperty(FILE_LENGTH_PROP) != null) {
      length = (Long) entity.getProperty(FILE_LENGTH_PROP);
    } else {
      AppEngineFile file = nameToAppEngineFile(filename);
      length = FILES.stat(file).getLength();
    }
    return new GcsFileMetadata(filename, options, null, length, creationTime);
  }

  @Override
  public Future<GcsFileMetadata> readObjectAsync(
      ByteBuffer dst, GcsFilename filename, long offset, long timeoutMillis) {
    Preconditions.checkArgument(offset >= 0, "%s: offset must be non-negative: %s", this, offset);
    try {
      GcsFileMetadata meta = getObjectMetadata(filename, timeoutMillis);
      if (meta == null) {
        return Futures.immediateFailedFuture(
            new FileNotFoundException(this + ": No such file: " + filename));
      }
      if (offset >= meta.getLength()) {
        return Futures.immediateFailedFuture(new BadRangeException(
            "The requested range cannot be satisfied. bytes=" + Long.toString(offset) + "-"
            + Long.toString(offset + dst.remaining()) + " the file is only " + meta.getLength()));
      }
      AppEngineFile file = nameToAppEngineFile(filename);
      try (FileReadChannel readChannel = FILES.openReadChannel(file, false)) {
        readChannel.position(offset);
        int read = 0;
        while (read != -1 && dst.hasRemaining()) {
          read = readChannel.read(dst);
        }
      }
      return Futures.immediateFuture(meta);
    } catch (IOException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public boolean deleteObject(GcsFilename filename, long timeoutMillis) throws IOException {
    Transaction tx = DATASTORE.beginTransaction();
    Key key = makeKey(filename);
    try {
      DATASTORE.get(tx, key);
      DATASTORE.delete(tx, key);
    } catch (EntityNotFoundException ex) {
      return false;
    } finally {
      if (tx.isActive()) {
        tx.commit();
      }
    }
    FILES.delete(nameToAppEngineFile(filename));
    return true;
  }

  @Override
  public int getChunkSizeBytes() {
    return CHUNK_ALIGNMENT_BYTES;
  }

  @Override
  public void putObject(GcsFilename filename, GcsFileOptions options, ByteBuffer content,
      long timeoutMillis) throws IOException {
    Token token = beginObjectCreation(filename, options, timeoutMillis);
    finishObjectCreation(token, content, timeoutMillis);
  }

  @Override
  public void composeObject(Iterable<String> source, GcsFilename dest, long timeoutMillis)
      throws IOException {
    int size = Iterables.size(source);
    if (size > 32) {
      throw new IOException("Compose attempted with too many components. Limit is 32");
    }
    if (size < 2) {
      throw new IOException("You must provide at least two source components.");
    }
    ByteBuffer chunk = ByteBuffer.allocate(1024);
    Token token = beginObjectCreation(dest, GcsFileOptions.getDefaultInstance(), timeoutMillis);
    for (String filename : source) {
      GcsFilename sourceFileName = new GcsFilename(dest.getBucketName(), filename);
      GcsFileMetadata meta = getObjectMetadata(sourceFileName, timeoutMillis);
      if (meta == null) {
        throw new FileNotFoundException(this + ": No such file: " + filename);
      }
      AppEngineFile file = nameToAppEngineFile(sourceFileName);
      try (FileReadChannel readChannel = FILES.openReadChannel(file, false)) {
        readChannel.position(0);
        while (readChannel.read(chunk) != -1) {
          chunk.flip();
          token = append(token, chunk);
          chunk.clear();
        }
      }
    }
    finishObjectCreation(token, ByteBuffer.allocate(0), timeoutMillis);
  }

  @Override
  public void copyObject(GcsFilename source, GcsFilename dest, GcsFileOptions fileOptions,
      long timeoutMillis) throws IOException {
    GcsFileMetadata meta = getObjectMetadata(source, timeoutMillis);
    if (meta == null) {
      throw new FileNotFoundException(this + ": No such file: " + source);
    }
    if (fileOptions == null) {
      fileOptions = meta.getOptions();
    }
    ByteBuffer chunk = ByteBuffer.allocate(1024);
    Token token = beginObjectCreation(dest, fileOptions, timeoutMillis);
    AppEngineFile file = nameToAppEngineFile(source);
    try (FileReadChannel readChannel = FILES.openReadChannel(file, false)) {
      readChannel.position(0);
      while (readChannel.read(chunk) != -1) {
        chunk.flip();
        token = append(token, chunk);
        chunk.clear();
      }
    }
    finishObjectCreation(token, ByteBuffer.allocate(0), timeoutMillis);
  }

  @Override
  public ListItemBatch list(String bucket, String prefix, String delimiter,
      String marker, int maxResults, long timeoutMillis) throws IOException {
    Query query = makeQuery(bucket);
    int prefixLength;
    if (!Strings.isNullOrEmpty(prefix)) {
      Key keyPrefix = makeKey(bucket, prefix);
      query.setFilter(new FilterPredicate(KEY_RESERVED_PROPERTY, GREATER_THAN_OR_EQUAL, keyPrefix));
      prefixLength = prefix.length();
    } else {
      prefixLength = 0;
    }
    FetchOptions fetchOptions = FetchOptions.Builder.withDefaults();
    if (marker != null) {
      fetchOptions.startCursor(Cursor.fromWebSafeString(marker));
    }
    List<ListItem> items = new ArrayList<>(maxResults);
    Set<String> prefixes = new HashSet<>();
    QueryResultIterator<Entity> dsResults =
        DATASTORE.prepare(query).asQueryResultIterator(fetchOptions);
    while (items.size() < maxResults && dsResults.hasNext()) {
      Entity entity = dsResults.next();
      String name = entity.getKey().getName();
      if (prefixLength > 0 && !name.startsWith(prefix)) {
        break;
      }
      if (!Strings.isNullOrEmpty(delimiter)) {
        int delimiterIdx = name.indexOf(delimiter, prefixLength);
        if (delimiterIdx > 0) {
          name = name.substring(0, delimiterIdx + 1);
          if (prefixes.add(name)) {
            items.add(new ListItem.Builder().setName(name).setDirectory(true).build());
          }
          continue;
        }
      }
      GcsFilename filename = new GcsFilename(bucket, name);
      GcsFileMetadata metadata = createGcsFileMetadata(entity, filename);
      ListItem listItem = new ListItem.Builder()
          .setName(name)
          .setLength(metadata.getLength())
          .setLastModified(metadata.getLastModified())
          .build();
      items.add(listItem);
    }
    Cursor cursor = dsResults.getCursor();
    String nextMarker = null;
    if (items.size() == maxResults && cursor != null) {
      nextMarker = cursor.toWebSafeString();
    }
    if (marker != null && marker.equals(nextMarker)) {
    }
    return new ListItemBatch(items, nextMarker);
  }

  @Override
  public int getMaxWriteSizeByte() {
    return 10_000_000;
  }
}

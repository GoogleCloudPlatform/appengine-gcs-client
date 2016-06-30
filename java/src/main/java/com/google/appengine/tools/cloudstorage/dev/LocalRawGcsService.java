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
import static com.google.common.io.BaseEncoding.base64Url;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.ThreadManager;
import com.google.appengine.api.blobstore.BlobInfo;
import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
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
import com.google.appengine.tools.cloudstorage.BadRangeException;
import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.ListItem;
import com.google.appengine.tools.cloudstorage.RawGcsService;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Delegate;
import com.google.apphosting.api.ApiProxy.Environment;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@code RawGcsService} for dev_appserver. For now, uses datastore and
 * fileService so that the viewers can be re-used.
 *
 * This class is not a perfect singleton. If methods are called concurrently from
 * different threads, it is possible that multiple instances of BlobStorage may be used.
 */
@SuppressWarnings("deprecation")
final class LocalRawGcsService implements RawGcsService {

  static final int CHUNK_ALIGNMENT_BYTES = 256 * 1024;

  private DatastoreService datastore;
  private BlobStorageAdapter blobStorage;
  private BlobstoreService blobstoreService;

  private static final String GOOGLE_STORAGE_FILE_KIND = "__GsFileInfo__";
  private static final String ENTITY_KIND_PREFIX = "_ah_FakeCloudStorage__";
  private static final String OPTIONS_PROP = "options";
  private static final String CREATION_TIME_PROP = "time";
  private static final String FILE_LENGTH_PROP = "length";
  private static final String BLOBSTORE_META_KIND = "__BlobInfo__";
  private static final String BLOBSTORE_KEY_PREFIX = "encoded_gs_key:";

  private static final HashMap<GcsFilename, List<ByteBuffer>> inMemoryData = new HashMap<>();

  private static class BlobStorageAdapter {

    private static final String BLOB_KEY_CLASS_NAME = BlobKey.class.getName();

    private static BlobStorageAdapter instance;

    private final Delegate<?> apiProxyDelegate;
    private final Object delegate;
    private final Method storeBlobMethod;
    private final Method fetchBlobMethod;
    private final Constructor<?> blobKeyConstructor;

    BlobStorageAdapter(Delegate<?> apiProxyDelegate) throws Exception {
      this.apiProxyDelegate = apiProxyDelegate;
      Method m = apiProxyDelegate.getClass().getDeclaredMethod("getService", String.class);
      m.setAccessible(true);
      Object bs = m.invoke(apiProxyDelegate, "blobstore");
      Field f = bs.getClass().getDeclaredField("blobStorage");
      f.setAccessible(true);
      delegate = f.get(bs);
      Class<?> delegateClass = delegate.getClass();
      Class<?> blobKeyClass = Class.forName(
          BLOB_KEY_CLASS_NAME, true, delegateClass.getClassLoader());
      blobKeyConstructor = blobKeyClass.getDeclaredConstructor(String.class);
      storeBlobMethod = delegateClass.getDeclaredMethod("storeBlob", blobKeyClass);
      storeBlobMethod.setAccessible(true);
      fetchBlobMethod = delegateClass.getDeclaredMethod("fetchBlob", blobKeyClass);
      fetchBlobMethod.setAccessible(true);
    }

    private Object getParam(BlobKey blobKey) throws IOException {
      if (blobKey.getClass() == storeBlobMethod.getParameterTypes()[0]) {
        return blobKey;
      }
      try {
        return blobKeyConstructor.newInstance(blobKey.getKeyString());
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    public OutputStream storeBlob(BlobKey blobKey) throws IOException {
      Object param = getParam(blobKey);
      try {
        return (OutputStream) storeBlobMethod.invoke(delegate, param);
      } catch (IllegalAccessException | IllegalArgumentException e) {
        throw new IllegalStateException("Failed to invoke blobStorage", e);
      } catch (InvocationTargetException e) {
        Throwable targetException = e.getTargetException();
        if (targetException instanceof IOException) {
          throw ((IOException) targetException);
        }
        throw new IOException(e);
      }
    }

    public InputStream fetchBlob(BlobKey blobKey) throws IOException {
      Object param = getParam(blobKey);
      try {
        return (InputStream) fetchBlobMethod.invoke(delegate, param);
      } catch (IllegalAccessException | IllegalArgumentException e) {
        throw new IllegalStateException("Failed to invoke blobStorage", e);
      } catch (InvocationTargetException e) {
        Throwable targetException = e.getTargetException();
        if (targetException instanceof IOException) {
          throw ((IOException) targetException);
        }
        throw new IOException(e);
      }
    }

    private static BlobStorageAdapter getInstance() throws IOException {
      Delegate<?> apiProxyDelegate = ApiProxy.getDelegate();
      if (instance == null || instance.apiProxyDelegate != apiProxyDelegate) {
        try {
          instance = new BlobStorageAdapter(apiProxyDelegate);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      return instance;
    }
  }

  private void ensureInitialized() throws IOException {
    blobStorage = BlobStorageAdapter.getInstance();
    blobstoreService = BlobstoreServiceFactory.getBlobstoreService();
    datastore = DatastoreServiceFactory.getDatastoreService();
  }

  static final class Token implements RawGcsCreationToken {
    private static final long serialVersionUID = 954846981243798905L;

    private final GcsFilename filename;
    private final GcsFileOptions options;
    private final long offset;

    Token(GcsFilename filename, GcsFileOptions options, long offset) {
      this.options = options;
      this.filename = checkNotNull(filename, "Null filename");
      this.offset = offset;
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
    ensureInitialized();
    inMemoryData.put(filename, new ArrayList<ByteBuffer>());
    return new Token(filename, options, 0);
  }

  private Token append(RawGcsCreationToken token, ByteBuffer chunk) {
    Token t = (Token) token;
    if (!chunk.hasRemaining()) {
      return t;
    }

    int chunksize = chunk.remaining();
    ByteBuffer inMemoryBuffer = ByteBuffer.allocate(chunksize);
    inMemoryBuffer.put(chunk);
    inMemoryBuffer.flip();
    inMemoryData.get(t.filename).add(inMemoryBuffer);

    return new Token(t.filename, t.options, t.offset + chunksize);
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
    try {
      ensureInitialized();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  private Key makeBlobstoreKey(GcsFilename filename) {
    String origNamespace = NamespaceManager.get();
    try {
      NamespaceManager.set("");
      return KeyFactory.createKey(
          null, BLOBSTORE_META_KIND, getBlobKeyForFilename(filename).getKeyString());
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

  private Query makeBlobstoreQuery() {
    String origNamespace = NamespaceManager.get();
    try {
      NamespaceManager.set("");
      return new Query(BLOBSTORE_META_KIND);
    } finally {
      NamespaceManager.set(origNamespace);
    }
  }

  @Override
  public void finishObjectCreation(RawGcsCreationToken token, ByteBuffer chunk, long timeoutMillis)
      throws IOException {
    ensureInitialized();
    Token t = append(token, chunk);

    int totalBytes = 0;

    BlobKey blobKey = getBlobKeyForFilename(t.filename);
    try (WritableByteChannel outputChannel = Channels.newChannel(blobStorage.storeBlob(blobKey))) {
      for (ByteBuffer buffer : inMemoryData.get(t.filename)){
        totalBytes += buffer.remaining();
        outputChannel.write(buffer);
      }
      inMemoryData.remove(t.filename);
    }

    String mimeType = t.options.getMimeType();
    if (Strings.isNullOrEmpty(mimeType)) {
      mimeType = "application/octet-stream";
    }

    BlobInfo blobInfo = new BlobInfo(
        blobKey, mimeType, new Date(), getPathForGcsFilename(t.filename),
        totalBytes);

    String namespace = NamespaceManager.get();
    try {
      NamespaceManager.set("");
      String blobKeyString = blobInfo.getBlobKey().getKeyString();
      Entity blobInfoEntity =
          new Entity(GOOGLE_STORAGE_FILE_KIND, blobKeyString);
      blobInfoEntity.setProperty(BlobInfoFactory.CONTENT_TYPE, blobInfo.getContentType());
      blobInfoEntity.setProperty(BlobInfoFactory.CREATION, blobInfo.getCreation());
      blobInfoEntity.setProperty(BlobInfoFactory.FILENAME, blobInfo.getFilename());
      blobInfoEntity.setProperty(BlobInfoFactory.SIZE, blobInfo.getSize());
      datastore.put(blobInfoEntity);
    } finally {
      NamespaceManager.set(namespace);
    }

    Entity e = new Entity(makeKey(t.filename));
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(t.options);
    }
    e.setUnindexedProperty(OPTIONS_PROP, new Blob(bout.toByteArray()));
    e.setUnindexedProperty(CREATION_TIME_PROP, System.currentTimeMillis());
    e.setUnindexedProperty(FILE_LENGTH_PROP, totalBytes);
    datastore.put(null, e);
  }

  @Override
  public GcsFileMetadata getObjectMetadata(GcsFilename filename, long timeoutMillis)
      throws IOException {
    ensureInitialized();
    Entity entity;
    try {
      entity = datastore.get(null, makeKey(filename));
      return createGcsFileMetadata(entity, filename);
    } catch (EntityNotFoundException ex1) {
      try {
        entity = datastore.get(null, makeBlobstoreKey(filename));
        return createGcsFileMetadataFromBlobstore(entity, filename);
      } catch (EntityNotFoundException ex2) {
        return null;
      }
    }
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
      ByteBuffer chunk = ByteBuffer.allocate(1024);

      long totalBytesRead = 0;
      try (ReadableByteChannel readChannel = Channels.newChannel(
          blobStorage.fetchBlob(getBlobKeyForFilename(filename)))) {
        long bytesRead = 0;
        bytesRead = readChannel.read(chunk);
        while (bytesRead != -1) {
          totalBytesRead += bytesRead;
          chunk.clear();
          bytesRead = readChannel.read(chunk);
        }
      }
      length = totalBytesRead;
    }
    return new GcsFileMetadata(filename, options, null, length, creationTime);
  }

  private GcsFileMetadata createGcsFileMetadataFromBlobstore(Entity entity, GcsFilename filename) {
    return new GcsFileMetadata(
        filename,
        GcsFileOptions.getDefaultInstance(),
        "",
        (Long) entity.getProperty("size"),
        (Date) entity.getProperty("creation"));
  }

  @Override
  public Future<GcsFileMetadata> readObjectAsync(
      ByteBuffer dst, GcsFilename filename, long offset, long timeoutMillis) {
    Preconditions.checkArgument(offset >= 0, "%s: offset must be non-negative: %s", this, offset);
    try {
      ensureInitialized();
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

      int read = 0;

      try (ReadableByteChannel readChannel = Channels.newChannel(
          blobStorage.fetchBlob(getBlobKeyForFilename(filename)))) {
        if (offset > 0) {
          long bytesRemaining = offset;
          ByteBuffer seekBuffer = ByteBuffer.allocate(1024);
          while (bytesRemaining > 0) {
            if (bytesRemaining < seekBuffer.limit()) {
              seekBuffer.limit((int) bytesRemaining);
            }
            read = readChannel.read(seekBuffer);
            if (read == -1) {
              return Futures.immediateFailedFuture(new BadRangeException(
                  "The requested range cannot be satisfied; seek failed with "
                  + Long.toString(bytesRemaining) + " bytes remaining."));

            }
            bytesRemaining -= read;
            seekBuffer.clear();
          }
          read = 0;
        }
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
    ensureInitialized();
    Transaction tx = datastore.beginTransaction();
    Key key = makeKey(filename);
    try {
      datastore.get(tx, key);
      datastore.delete(tx, key);
      blobstoreService.delete(getBlobKeyForFilename(filename));
    } catch (EntityNotFoundException ex) {
      return false;
    } finally {
      if (tx.isActive()) {
        tx.commit();
      }
    }

    return true;
  }

  private String getPathForGcsFilename(GcsFilename filename) {
    return new StringBuilder()
        .append("/gs/")
        .append(filename.getBucketName())
        .append('/')
        .append(filename.getObjectName())
        .toString();
  }

  private BlobKey getBlobKeyForFilename(GcsFilename filename) {
    return blobstoreService.createGsBlobKey(getPathForGcsFilename(filename));
  }

  @Override
  public int getChunkSizeBytes() {
    return CHUNK_ALIGNMENT_BYTES;
  }

  @Override
  public void putObject(GcsFilename filename, GcsFileOptions options, ByteBuffer content,
      long timeoutMillis) throws IOException {
    ensureInitialized();
    Token token = beginObjectCreation(filename, options, timeoutMillis);
    finishObjectCreation(token, content, timeoutMillis);
  }

  @Override
  public void composeObject(Iterable<String> source, GcsFilename dest, long timeoutMillis)
      throws IOException {
    ensureInitialized();
    int size = Iterables.size(source);
    if (size > 32) {
      throw new IOException("Compose attempted with too many components. Limit is 32");
    }
    if (size < 2) {
      throw new IOException("You must provide at least two source components.");
    }
    Token token = beginObjectCreation(dest, GcsFileOptions.getDefaultInstance(), timeoutMillis);

    for (String filename : source) {
      GcsFilename sourceGcsFilename = new GcsFilename(dest.getBucketName(), filename);
      appendFileContentsToToken(sourceGcsFilename, token);
    }
    finishObjectCreation(token, ByteBuffer.allocate(0), timeoutMillis);
  }

  private Token appendFileContentsToToken(GcsFilename source, Token token) throws IOException {
    ByteBuffer chunk = ByteBuffer.allocate(1024);

    try (ReadableByteChannel readChannel = Channels.newChannel(
        blobStorage.fetchBlob(getBlobKeyForFilename(source)))) {
      while (readChannel.read(chunk) != -1) {
        chunk.flip();
        token = append(token, chunk);
        chunk.clear();
      }
    }
    return token;
  }

  @Override
  public void copyObject(GcsFilename source, GcsFilename dest, GcsFileOptions fileOptions,
      long timeoutMillis) throws IOException {
    ensureInitialized();
    GcsFileMetadata meta = getObjectMetadata(source, timeoutMillis);
    if (meta == null) {
      throw new FileNotFoundException(this + ": No such file: " + source);
    }
    if (fileOptions == null) {
      fileOptions = meta.getOptions();
    }

    Token token = beginObjectCreation(dest, fileOptions, timeoutMillis);
    appendFileContentsToToken(source, token);
    finishObjectCreation(token, ByteBuffer.allocate(0), timeoutMillis);
  }

  @Override
  public ListItemBatch list(String bucket, String prefix, String delimiter,
      String marker, int maxResults, long timeoutMillis) throws IOException {
    ensureInitialized();
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
        datastore.prepare(query).asQueryResultIterator(fetchOptions);
    boolean fromBlobstore = false;
    if (!dsResults.hasNext()) {
      // If no results, perhaps there's metadata stored by the Blobstore API that we missed.
      // Note that Blobstore metadata results aren't returned if some blobs are uploaded via
      // Blobstore and others are uploaded using this client library.  We only check for Blobstore
      // metadata if no results are found in appengine-gcs-client's metadata store.
      Query blobstoreQuery = makeBlobstoreQuery();
      dsResults = datastore.prepare(blobstoreQuery).asQueryResultIterator(fetchOptions);
      fromBlobstore = true;
    }
    while (items.size() < maxResults && dsResults.hasNext()) {
      Entity entity = dsResults.next();
      String name;
      if (!fromBlobstore) {
        name = entity.getKey().getName();
      } else {
        String encodedName = entity.getKey().getName().substring(BLOBSTORE_KEY_PREFIX.length());
        String fullName = new String(base64Url().omitPadding().decode(encodedName));
        String[] nameInfo = fullName.split("/", 4);
        name = nameInfo[nameInfo.length - 1];
      }
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
      GcsFileMetadata metadata;
      if (!fromBlobstore) {
        metadata = createGcsFileMetadata(entity, filename);
      } else {
        metadata = createGcsFileMetadataFromBlobstore(entity, filename);
      }
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
    return new ListItemBatch(items, nextMarker);
  }

  @Override
  public int getMaxWriteSizeByte() {
    return 10_000_000;
  }
}

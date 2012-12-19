package com.google.appengine.tools.cloudstorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A writable byte channel for writing data to GoogleStorage. Write calls will either place the data
 * into an internal buffer or make a synchronous RPC call to write the data.
 *
 * Calling {@link #close()} is mandatory. This will cause any buffers to be flushed and all data
 * written to be stored durably.
 *
 * Implementations of this class may buffer data internally to reduce remote calls.
 */
public interface GcsOutputChannel extends WritableByteChannel {

  /**
   * Returns the filename.
   */
  GcsFilename getFilename();

  /**
   * @return The size of the buffer used internally by this class. (0 if the data is unbuffered)
   */
  int getBufferSizeBytes();

  /**
   * @see WritableByteChannel#write(ByteBuffer)
   *
   * @param src A byte buffer that should be written to the end of the file. This buffer may be of
   *        any size, but writes are not guaranteed to be durable until close is called.
   *
   * @throws IOException An error occurred writing the data. If an IOException is thrown none or
   *         part of the data may have been written. For this reason it may be best to start writing
   *         the file from the beginning. This can be avoided by providing a retry policy when
   *         constructing this class.
   * @return Will always write (or buffer) the full buffer passed in. As such it will return the
   *         size of the provided buffer.
   */
  @Override
  public int write(ByteBuffer src) throws IOException;

}

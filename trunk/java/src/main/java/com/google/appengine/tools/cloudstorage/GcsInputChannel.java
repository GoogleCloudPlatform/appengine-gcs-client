package com.google.appengine.tools.cloudstorage;

import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;

/**
 * A readable byte channel for reading data to Google Cloud Storage.
 *
 *  Implementations of this class may further buffer data internally to reduce remote calls.
 *
 *  This class is @{link Serializable}, which allows for reading part of a file, serializing the
 * GcsInputChannel deserializing it, and continuing to read from the same file from teh same
 * position.
 */
public interface GcsInputChannel extends ReadableByteChannel, Serializable {
}

/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

package com.google.appengine.tools.cloudstorage.oauth;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.xml.stream.XMLStreamConstants.CDATA;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 * A simpler XML handler.
 * Handler is constructed with byte array content and a set of paths.
 * Each matching path is represented as a collection of strings.
 * Each next call would return an XmlEvent the content of an XML element that matched
 * any part of the given paths.
 * Matching is done against the closing XML element (this allows an easier way to group elements).
 */
public final class XmlHandler {

  private final XMLEventReader xmlr;
  private final PathTrie prefixTrie;
  private final LinkedList<String> currentPath = new LinkedList<>();
  private final LinkedList<StringBuilder> pathContent = new LinkedList<>();
  private XmlHandler.XmlEvent next;

  /**
   * The type of the XmlEvent.
   */
  public enum EventType {
    OPEN_ELEMENT,
    CLOSE_ELEMENT
  }

  /**
   * Information associated with the callback event.
   */
  public static final class XmlEvent {

    private final EventType eventType;
    private final String name;
    private final String value;

    XmlEvent(EventType eventType, List<String> path, String value) {
      this.eventType = eventType;
      this.name =  path.isEmpty() ? "" : path.get(path.size() - 1);
      this.value = value;
    }

    public EventType getEventType() {
      return eventType;
    }

    public String getName() {
      return name;
    }

    /**
     * Returns an XML element's text (only available for {@link EventType#CLOSE_ELEMENT} events).
     */
    public String getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      XmlEvent other = (XmlEvent) obj;
      return Objects.equals(name, other.name) && Objects.equals(value, other.value);
    }

    @Override
    public String toString() {
      return "XmlValue [name=" + name + ", value=" + value + "]";
    }
  }

  XmlHandler(byte[] content, Set<? extends Iterable<String>> paths)
      throws XMLStreamException {
    prefixTrie = new PathTrie(paths);
    Reader reader = new InputStreamReader(new ByteArrayInputStream(content), UTF_8);
    XMLInputFactory f = XMLInputFactory.newInstance();
    xmlr = f.createXMLEventReader(reader);
  }

  public boolean hasNext() throws XMLStreamException {
    if (next == null) {
      next = next();
    }
    return next != null;
  }

  public XmlHandler.XmlEvent next() throws XMLStreamException {
    while (next == null && xmlr.hasNext()) {
      XMLEvent event = xmlr.nextEvent();
      switch (event.getEventType()) {
        case START_ELEMENT:
          currentPath.add(event.asStartElement().getName().getLocalPart());
          pathContent.add(new StringBuilder());
          if (prefixTrie.contains(currentPath)) {
            next = new XmlEvent(EventType.OPEN_ELEMENT, currentPath, null);
          }
          break;
        case END_ELEMENT:
          if (prefixTrie.contains(currentPath)) {
            String text = pathContent.getLast().toString().trim();
            next = new XmlEvent(EventType.CLOSE_ELEMENT, currentPath, text);
          }
          currentPath.removeLast();
          pathContent.removeLast();
          break;
        case CDATA:
        case CHARACTERS:
          pathContent.getLast().append(event.asCharacters().getData());
          break;
        default:
          break;
      }
    }
    try {
      return next;
    } finally {
      next = null;
    }
  }
}
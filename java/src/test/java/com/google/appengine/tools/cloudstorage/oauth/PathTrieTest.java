package com.google.appengine.tools.cloudstorage.oauth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.List;


/**
 * Tests {@link PathTrie}
 */
@RunWith(JUnit4.class)
public class PathTrieTest {

  private List<String> path1 = ImmutableList.of("root1");
  private List<String> path2 = ImmutableList.of("root2");
  private List<String> path3 = ImmutableList.of("root1", "folder1");
  private List<String> path4 = ImmutableList.of("root1", "folder2");
  private List<String> path5 = ImmutableList.of("root1", "folder1", "folder2");
  private List<String> path6 = ImmutableList.of("root1", "folder2", "folder3");
  private List<String> path7 = ImmutableList.of("root1", "folder2", "folder4");
  private List<String> path8 = ImmutableList.of("root1", "folder3");
  private List<String> path9 = ImmutableList.of("root2", "folder3");

  @Test
  public void testStartFromEmpty() {
    PathTrie trie = new PathTrie(Collections.<Iterable<String>>emptySet());
    assertTrue(trie.contains(Collections.<String>emptyList()));
    assertFalse(trie.contains(path1));
    trie.add(path1);
    assertTrue(trie.contains(path1));
    assertFalse(trie.contains(path3));
    trie.add(path5);
    assertTrue(trie.contains(path3));
    assertTrue(trie.contains(path5));
    assertFalse(trie.contains(path2));
  }

  @Test
  public void testStartFromNonEmpty() {
    PathTrie trie = new PathTrie(ImmutableSet.of(path5, path6));
    assertTrue(trie.contains(path1));
    assertFalse(trie.contains(path2));
    assertTrue(trie.contains(path3));
    assertTrue(trie.contains(path4));
    assertTrue(trie.contains(path5));
    assertTrue(trie.contains(path6));
    assertFalse(trie.contains(path7));
    assertFalse(trie.contains(path8));
    trie.add(path7);
    assertTrue(trie.contains(path7));
    assertFalse(trie.contains(path8));
    trie.add(path8);
    assertTrue(trie.contains(path8));
    assertFalse(trie.contains(path2));
    assertFalse(trie.contains(path9));
    trie.add(path9);
    assertTrue(trie.contains(path2));
    assertTrue(trie.contains(path9));
  }
}

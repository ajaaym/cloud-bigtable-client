// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/admin/v2/bigtable_table_admin.proto

package com.google.bigtable.admin.v2;

public interface CreateTableRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:google.bigtable.admin.v2.CreateTableRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string name = 1;</code>
   *
   * <pre>
   * The unique name of the instance in which to create the table.
   * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
   * </pre>
   */
  java.lang.String getName();
  /**
   * <code>optional string name = 1;</code>
   *
   * <pre>
   * The unique name of the instance in which to create the table.
   * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
   * </pre>
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
   * <code>optional string table_id = 2;</code>
   *
   * <pre>
   * The name by which the new table should be referred to within the parent
   * instance, e.g. "foobar" rather than "&lt;parent&gt;/tables/foobar".
   * </pre>
   */
  java.lang.String getTableId();
  /**
   * <code>optional string table_id = 2;</code>
   *
   * <pre>
   * The name by which the new table should be referred to within the parent
   * instance, e.g. "foobar" rather than "&lt;parent&gt;/tables/foobar".
   * </pre>
   */
  com.google.protobuf.ByteString
      getTableIdBytes();

  /**
   * <code>optional .google.bigtable.admin.v2.Table table = 3;</code>
   *
   * <pre>
   * The Table to create.
   * </pre>
   */
  boolean hasTable();
  /**
   * <code>optional .google.bigtable.admin.v2.Table table = 3;</code>
   *
   * <pre>
   * The Table to create.
   * </pre>
   */
  com.google.bigtable.admin.v2.Table getTable();
  /**
   * <code>optional .google.bigtable.admin.v2.Table table = 3;</code>
   *
   * <pre>
   * The Table to create.
   * </pre>
   */
  com.google.bigtable.admin.v2.TableOrBuilder getTableOrBuilder();

  /**
   * <code>repeated .google.bigtable.admin.v2.CreateTableRequest.Split initial_splits = 4;</code>
   *
   * <pre>
   * The optional list of row keys that will be used to initially split the
   * table into several tablets (Tablets are similar to HBase regions).
   * Given two split keys, "s1" and "s2", three tablets will be created,
   * spanning the key ranges: [, s1), [s1, s2), [s2, ).
   * Example:
   *  * Row keys := ["a", "apple", "custom", "customer_1", "customer_2",
   *                 "other", "zz"]
   *  * initial_split_keys := ["apple", "customer_1", "customer_2", "other"]
   *  * Key assignment:
   *    - Tablet 1 [, apple)                =&gt; {"a"}.
   *    - Tablet 2 [apple, customer_1)      =&gt; {"apple", "custom"}.
   *    - Tablet 3 [customer_1, customer_2) =&gt; {"customer_1"}.
   *    - Tablet 4 [customer_2, other)      =&gt; {"customer_2"}.
   *    - Tablet 5 [other, )                =&gt; {"other", "zz"}.
   * </pre>
   */
  java.util.List<com.google.bigtable.admin.v2.CreateTableRequest.Split> 
      getInitialSplitsList();
  /**
   * <code>repeated .google.bigtable.admin.v2.CreateTableRequest.Split initial_splits = 4;</code>
   *
   * <pre>
   * The optional list of row keys that will be used to initially split the
   * table into several tablets (Tablets are similar to HBase regions).
   * Given two split keys, "s1" and "s2", three tablets will be created,
   * spanning the key ranges: [, s1), [s1, s2), [s2, ).
   * Example:
   *  * Row keys := ["a", "apple", "custom", "customer_1", "customer_2",
   *                 "other", "zz"]
   *  * initial_split_keys := ["apple", "customer_1", "customer_2", "other"]
   *  * Key assignment:
   *    - Tablet 1 [, apple)                =&gt; {"a"}.
   *    - Tablet 2 [apple, customer_1)      =&gt; {"apple", "custom"}.
   *    - Tablet 3 [customer_1, customer_2) =&gt; {"customer_1"}.
   *    - Tablet 4 [customer_2, other)      =&gt; {"customer_2"}.
   *    - Tablet 5 [other, )                =&gt; {"other", "zz"}.
   * </pre>
   */
  com.google.bigtable.admin.v2.CreateTableRequest.Split getInitialSplits(int index);
  /**
   * <code>repeated .google.bigtable.admin.v2.CreateTableRequest.Split initial_splits = 4;</code>
   *
   * <pre>
   * The optional list of row keys that will be used to initially split the
   * table into several tablets (Tablets are similar to HBase regions).
   * Given two split keys, "s1" and "s2", three tablets will be created,
   * spanning the key ranges: [, s1), [s1, s2), [s2, ).
   * Example:
   *  * Row keys := ["a", "apple", "custom", "customer_1", "customer_2",
   *                 "other", "zz"]
   *  * initial_split_keys := ["apple", "customer_1", "customer_2", "other"]
   *  * Key assignment:
   *    - Tablet 1 [, apple)                =&gt; {"a"}.
   *    - Tablet 2 [apple, customer_1)      =&gt; {"apple", "custom"}.
   *    - Tablet 3 [customer_1, customer_2) =&gt; {"customer_1"}.
   *    - Tablet 4 [customer_2, other)      =&gt; {"customer_2"}.
   *    - Tablet 5 [other, )                =&gt; {"other", "zz"}.
   * </pre>
   */
  int getInitialSplitsCount();
  /**
   * <code>repeated .google.bigtable.admin.v2.CreateTableRequest.Split initial_splits = 4;</code>
   *
   * <pre>
   * The optional list of row keys that will be used to initially split the
   * table into several tablets (Tablets are similar to HBase regions).
   * Given two split keys, "s1" and "s2", three tablets will be created,
   * spanning the key ranges: [, s1), [s1, s2), [s2, ).
   * Example:
   *  * Row keys := ["a", "apple", "custom", "customer_1", "customer_2",
   *                 "other", "zz"]
   *  * initial_split_keys := ["apple", "customer_1", "customer_2", "other"]
   *  * Key assignment:
   *    - Tablet 1 [, apple)                =&gt; {"a"}.
   *    - Tablet 2 [apple, customer_1)      =&gt; {"apple", "custom"}.
   *    - Tablet 3 [customer_1, customer_2) =&gt; {"customer_1"}.
   *    - Tablet 4 [customer_2, other)      =&gt; {"customer_2"}.
   *    - Tablet 5 [other, )                =&gt; {"other", "zz"}.
   * </pre>
   */
  java.util.List<? extends com.google.bigtable.admin.v2.CreateTableRequest.SplitOrBuilder> 
      getInitialSplitsOrBuilderList();
  /**
   * <code>repeated .google.bigtable.admin.v2.CreateTableRequest.Split initial_splits = 4;</code>
   *
   * <pre>
   * The optional list of row keys that will be used to initially split the
   * table into several tablets (Tablets are similar to HBase regions).
   * Given two split keys, "s1" and "s2", three tablets will be created,
   * spanning the key ranges: [, s1), [s1, s2), [s2, ).
   * Example:
   *  * Row keys := ["a", "apple", "custom", "customer_1", "customer_2",
   *                 "other", "zz"]
   *  * initial_split_keys := ["apple", "customer_1", "customer_2", "other"]
   *  * Key assignment:
   *    - Tablet 1 [, apple)                =&gt; {"a"}.
   *    - Tablet 2 [apple, customer_1)      =&gt; {"apple", "custom"}.
   *    - Tablet 3 [customer_1, customer_2) =&gt; {"customer_1"}.
   *    - Tablet 4 [customer_2, other)      =&gt; {"customer_2"}.
   *    - Tablet 5 [other, )                =&gt; {"other", "zz"}.
   * </pre>
   */
  com.google.bigtable.admin.v2.CreateTableRequest.SplitOrBuilder getInitialSplitsOrBuilder(
      int index);
}
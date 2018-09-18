/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;

import java.util.Collections;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class TestHBaseMutationAdapter {
  private static final String PROJECT_ID = "test-project-id";
  private static final String INSTANCE_ID = "test-instance-id";
  private static final String TABLE_ID = "test-table-id";
  public static final String APP_PROFILE_ID = "test-app-profile-id";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Mock
  private MutationAdapter<Delete> deleteAdapter;
  @Mock
  private MutationAdapter<Put> putAdapter;
  @Mock
  private MutationAdapter<Increment> incrementAdapter;
  @Mock
  private MutationAdapter<Append> appendAdapter;
  @Mock
  private RequestContext requestContext;
  @Mock
  private InstanceName instanceName;

  private HBaseMutationAdapter adapter;
  private DataGenerationHelper dataHelper = new DataGenerationHelper();

  private com.google.cloud.bigtable.data.v2.models.Mutation mutation =
      com.google.cloud.bigtable.data.v2.models.Mutation.create();
  public static class UnknownMutation extends Mutation {}

  private static final List<com.google.bigtable.v2.Mutation> EMPTY_MUTATIONS =
      Collections.emptyList();

  private byte[] rowKey;
  private RowMutation rowMutation;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    adapter = new HBaseMutationAdapter(deleteAdapter, putAdapter, incrementAdapter, appendAdapter);
    Mockito.when(instanceName.getProject()).thenReturn(PROJECT_ID);
    Mockito.when(instanceName.getInstance()).thenReturn(INSTANCE_ID);
    Mockito.when(requestContext.getInstanceName()).thenReturn(instanceName);
    Mockito.when(requestContext.getAppProfileId()).thenReturn(APP_PROFILE_ID);
    rowKey = dataHelper.randomData("rk1");
    rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
  }

  @After
  public void tearDown() {
    Mockito.verifyNoMoreInteractions(
        deleteAdapter,
        putAdapter,
        incrementAdapter,
        appendAdapter);
  }

  @Test
  public void testPutIsAdapted() {
    Put put = new Put(rowKey);
    adapter.adaptMutations(put, rowMutation);

    Mockito.verify(putAdapter, Mockito.times(1))
        .adaptMutations(
            Mockito.any(Put.class),
            Mockito.eq(rowMutation));
  }

  @Test
  public void testDeleteIsAdapted() {
    Delete delete = new Delete(rowKey);
    adapter.adaptMutations(delete, rowMutation);

    Mockito.verify(deleteAdapter, Mockito.times(1))
        .adaptMutations(
            Mockito.any(Delete.class),
            Mockito.eq(rowMutation));
  }

  @Test
  public void testAppendIsAdapted() {
    Append append = new Append(rowKey);

    adapter.adaptMutations(append, rowMutation);

    Mockito.verify(appendAdapter, Mockito.times(1))
        .adaptMutations(
            Mockito.any(Append.class),
            Mockito.eq(rowMutation));
  }

  @Test
  public void testIncrementIsAdapted() {
    Increment increment = new Increment(rowKey);

    adapter.adaptMutations(increment, rowMutation);

    Mockito.verify(incrementAdapter, Mockito.times(1))
        .adaptMutations(
            Mockito.any(Increment.class),
            Mockito.eq(rowMutation));
  }

  @Test
  public void exceptionIsThrownOnUnknownMutation() {
    UnknownMutation unknownMutation = new UnknownMutation();

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot adapt mutation of type");
    expectedException.expectMessage("UnknownMutation");

    adapter.adaptMutations(unknownMutation, rowMutation);
  }
}

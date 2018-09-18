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

import java.util.concurrent.TimeUnit;

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.MutationCase;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class TestDeleteAdapter {
  private static final String PROJECT_ID = "test-project-id";
  private static final String INSTANCE_ID = "test-instance-id";
  private static final String TABLE_ID = "test-table-id";
  public static final String APP_PROFILE_ID = "test-app-profile-id";
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected DeleteAdapter deleteAdapter = new DeleteAdapter();
  protected QualifierTestHelper qualifierTestHelper = new QualifierTestHelper();
  protected DataGenerationHelper randomHelper = new DataGenerationHelper();

  @Mock
  private RequestContext requestContext;
  @Mock
  private InstanceName instanceName;


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    Mockito.when(instanceName.getProject()).thenReturn(PROJECT_ID);
    Mockito.when(instanceName.getInstance()).thenReturn(INSTANCE_ID);
    Mockito.when(requestContext.getInstanceName()).thenReturn(instanceName);
    Mockito.when(requestContext.getAppProfileId()).thenReturn(APP_PROFILE_ID);
  }

  @Test
  public void testFullRowDelete() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey);
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation);
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);

    Assert.assertArrayEquals(rowKey, mutateRowRequest.getRowKey().toByteArray());
    Assert.assertEquals(1, mutateRowRequest.getMutationsCount());

    Mutation.MutationCase mutationCase = mutateRowRequest.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_ROW, mutationCase);

    testTwoWayAdapt(delete, deleteAdapter);
  }

  @Test
  public void testDeleteRowAtTimestampIsUnsupported() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey, 1000L);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform row deletion at timestamp");

    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation);
  }

  @Test
  public void testColumnFamilyDelete() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    Delete delete = new Delete(rowKey);
    delete.addFamily(family);
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation);
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);

    Assert.assertArrayEquals(rowKey, mutateRowRequest.getRowKey().toByteArray());
    Assert.assertEquals(1, mutateRowRequest.getMutationsCount());

    MutationCase mutationCase = mutateRowRequest.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_FAMILY, mutationCase);

    Mutation.DeleteFromFamily deleteFromFamily =
        mutateRowRequest.getMutations(0).getDeleteFromFamily();
    Assert.assertArrayEquals(family, deleteFromFamily.getFamilyNameBytes().toByteArray());

    testTwoWayAdapt(delete, deleteAdapter);
  }

  @Test
  public void testColumnFamilyDeleteAtTimestampFails() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey);
    delete.addFamily(Bytes.toBytes("family1"), 10000L);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform column family deletion before timestamp");

    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation);
  }

  @Test
  public void testDeleteColumnAtTimestamp() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");
    long hbaseTimestamp = 1000L;
    long bigtableStartTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp);
    long bigtableEndTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp + 1);

    Delete delete = new Delete(rowKey);
    delete.addColumn(family, qualifier, hbaseTimestamp);
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation);
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);

    Assert.assertArrayEquals(rowKey, mutateRowRequest.getRowKey().toByteArray());
    Assert.assertEquals(1, mutateRowRequest.getMutationsCount());

    MutationCase mutationCase = mutateRowRequest.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_COLUMN, mutationCase);

    Mutation.DeleteFromColumn deleteFromColumn =
        mutateRowRequest.getMutations(0).getDeleteFromColumn();
    Assert.assertArrayEquals(family, deleteFromColumn.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier, deleteFromColumn.getColumnQualifier().toByteArray());
    Assert.assertTrue(mutateRowRequest.getMutations(0).getDeleteFromColumn().hasTimeRange());

    TimestampRange timeStampRange = deleteFromColumn.getTimeRange();
    Assert.assertEquals(bigtableStartTimestamp, timeStampRange.getStartTimestampMicros());
    Assert.assertEquals(bigtableEndTimestamp, timeStampRange.getEndTimestampMicros());

    testTwoWayAdapt(delete, deleteAdapter);
  }

  @Test
  public void testDeleteLatestColumnThrows() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");

    Delete delete = new Delete(rowKey);
    delete.addColumn(family, qualifier);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot delete single latest cell");

    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation);
  }

  @Test
  public void testDeleteColumnBeforeTimestamp() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");
    long hbaseTimestamp = 1000L;
    long bigtableTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp + 1);

    Delete delete = new Delete(rowKey);
    delete.addColumns(family, qualifier, hbaseTimestamp);
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation);
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);

    Assert.assertArrayEquals(rowKey, mutateRowRequest.getRowKey().toByteArray());
    Assert.assertEquals(1, mutateRowRequest.getMutationsCount());
    Assert.assertEquals(
        MutationCase.DELETE_FROM_COLUMN, mutateRowRequest.getMutations(0).getMutationCase());

    Mutation.DeleteFromColumn deleteFromColumn =
        mutateRowRequest.getMutations(0).getDeleteFromColumn();
    Assert.assertArrayEquals(qualifier, deleteFromColumn.getColumnQualifier().toByteArray());
    Assert.assertTrue(mutateRowRequest.getMutations(0).getDeleteFromColumn().hasTimeRange());

    TimestampRange timeRange = deleteFromColumn.getTimeRange();
    Assert.assertEquals(0L, timeRange.getStartTimestampMicros());
    Assert.assertEquals(bigtableTimestamp, timeRange.getEndTimestampMicros());

    testTwoWayAdapt(delete, deleteAdapter);
  }

  @Test
  public void testDeleteFamilyVersionIsUnsupported() {
    // Unexpected to see this:
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    long hbaseTimestamp = 1000L;

    Delete delete = new Delete(rowKey);
    delete.addFamilyVersion(family, hbaseTimestamp);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform column family deletion at timestamp");

    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation);
  }

  /**
   * Convert the {@link Delete} to a {@link Mutation}, back to a {@link Delete}, back to
   * {@link Mutation}. Compare the two mutations for equality. This ensures that the adapt
   * process is idempotent.
   */
  private void testTwoWayAdapt(Delete delete, DeleteAdapter adapter) {
    byte[] rowKey = randomHelper.randomData("rk1-");
    RowMutation rowMutation1 = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    deleteAdapter.adapt(delete, rowMutation1);
    // delete -> mutation
    MutateRowRequest firstAdapt = rowMutation1.toProto(requestContext);
    // mutation -> delete -> mutation;
    RowMutation rowMutation2 = RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey));
    adapter.adapt(adapter.adapt(firstAdapt), rowMutation2);
    MutateRowRequest secondAdapt = rowMutation2.toProto(requestContext);
        // The round trips
    Assert.assertEquals(firstAdapt, secondAdapt);
  }
}
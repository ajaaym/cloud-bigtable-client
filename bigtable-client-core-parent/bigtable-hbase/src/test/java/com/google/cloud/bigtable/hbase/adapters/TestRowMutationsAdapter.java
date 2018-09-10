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


import com.google.bigtable.v2.MutateRowRequest;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import com.google.protobuf.ByteString;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;


import static org.mockito.Mockito.doReturn;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(JUnit4.class)
public class TestRowMutationsAdapter {

  @Mock
  private MutationAdapter<org.apache.hadoop.hbase.client.Mutation> mutationAdapter;

  private RowMutationsAdapter adapter;
  private DataGenerationHelper dataHelper = new DataGenerationHelper();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    adapter = Mockito.spy(new RowMutationsAdapter(mutationAdapter));
  }

  @Test
  public void testRowKeyIsSet() {
    byte[] rowKey = dataHelper.randomData("rk-1");
    RowMutations mutations = new RowMutations(rowKey);
    MutateRowRequest.Builder result = adapter.adapt(mutations);
    Assert.assertArrayEquals(rowKey, result.getRowKey().toByteArray());
  }

  @Test
  public void testMultipleMutationsAreAdapted() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk-1");
    RowMutations mutations = new RowMutations(rowKey);

    byte[] family1 = dataHelper.randomData("cf1");
    byte[] qualifier1 = dataHelper.randomData("qualifier1");
    byte[] value1 = dataHelper.randomData("value1");

    byte[] family2 = dataHelper.randomData("cf2");
    byte[] qualifier2 = dataHelper.randomData("qualifier2");
    byte[] value2 = dataHelper.randomData("value2");

    mutations.add(
        new Put(rowKey)
            .addColumn(family1, qualifier1, value1));

    mutations.add(
        new Put(rowKey)
            .addColumn(family2, qualifier2, value2));

    // When mockAdapter is asked to adapt the above mutations, we'll return these responses:

    com.google.cloud.bigtable.data.v2.models.Mutation mutation = com.google.cloud.bigtable.data.v2.models.Mutation.create()
        .setCell(ByteString.copyFrom(family1).toStringUtf8(),
            ByteString.copyFrom(qualifier1),
            ByteString.copyFrom(value1))
        .setCell(ByteString.copyFrom(family2).toStringUtf8(),
            ByteString.copyFrom(qualifier2),
            ByteString.copyFrom(value2));

    doReturn(mutation).when(adapter).newMutationBuilder();

    // Adapt the RowMutations to a RowMutation:
    MutateRowRequest.Builder result = adapter.adapt(mutations);
    Mockito
        .verify(mutationAdapter,times(2))
        .adaptMutations(Matchers.any(org.apache.hadoop.hbase.client.Mutation.class),
            Matchers.eq(mutation));

    Assert.assertArrayEquals(rowKey, result.getRowKey().toByteArray());

    // Verify mutations.getMutations(0) is in the first position in result.mods.
    Assert.assertArrayEquals(family1,
        result.getMutations(0).getSetCell().getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier1,
        result.getMutations(0).getSetCell().getColumnQualifier().toByteArray());

    //Verify mutations.getMutation(1) is in the second position in result.mods.
    Assert.assertArrayEquals(family2,
        result.getMutations(1).getSetCell().getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier2,
        result.getMutations(1).getSetCell().getColumnQualifier().toByteArray());
  }
}

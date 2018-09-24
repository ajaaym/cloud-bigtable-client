package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.ClientWrapper;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class BigtableDataClientWrapper implements ClientWrapper {

  private final String clientDefaultAppProfileId;
  private final RequestContext requestContext;
  private final BigtableDataClient bigtableDataClient;

  public BigtableDataClientWrapper(BigtableOptions bigtableOptions) throws IOException {
    this.clientDefaultAppProfileId = bigtableOptions.getAppProfileId();
    InstanceName instanceName = InstanceName.of(bigtableOptions.getProjectId(), bigtableOptions.getInstanceId());
    BigtableDataSettings bigtableDataSettings =
        BigtableDataSettings.newBuilder()
        .setInstanceName(instanceName)
        .build();
    this.bigtableDataClient = BigtableDataClient.create(bigtableDataSettings);
    this.requestContext = RequestContext.create(instanceName, clientDefaultAppProfileId);
  }

  @Override
  public void mutateRow(RowMutation rowMutation) throws ExecutionException, InterruptedException {
    bigtableDataClient.mutateRowAsync(rowMutation).get();
  }

  @Override public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation)
      throws InterruptedException {
    return bigtableDataClient.mutateRowAsync(rowMutation);
  }

  @Override public Row readModifyWriteRow(ReadModifyWriteRow mutation)
      throws ExecutionException, InterruptedException {
    return bigtableDataClient.readModifyWriteRowAsync(mutation).get();
  }

  @Override public ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow mutation)
      throws InterruptedException {
    return bigtableDataClient.readModifyWriteRowAsync(mutation);
  }

  @Override public BulkMutation createBulkMutationBatcher() {
    return new BulkMutationWrapper(bigtableDataClient.newBulkMutationBatcher());
  }

  @Override public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation mutation) {
    return bigtableDataClient.checkAndMutateRowAsync(mutation);
  }

  @Override public Boolean checkAndMutateRow(ConditionalRowMutation mutation)
      throws ExecutionException, InterruptedException {
    return bigtableDataClient.checkAndMutateRowAsync(mutation).get();
  }
}

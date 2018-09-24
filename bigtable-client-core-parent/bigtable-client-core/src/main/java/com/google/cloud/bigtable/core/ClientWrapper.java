package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;

public interface ClientWrapper {
  void mutateRow(RowMutation rowMutation) throws ExecutionException, InterruptedException;
  ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) throws InterruptedException;
  Row readModifyWriteRow(ReadModifyWriteRow mutation)
      throws ExecutionException, InterruptedException;
  ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow mutation) throws InterruptedException;
  BulkMutation createBulkMutationBatcher();
  ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation mutation);
  Boolean checkAndMutateRow(ConditionalRowMutation mutation)
      throws ExecutionException, InterruptedException;
}

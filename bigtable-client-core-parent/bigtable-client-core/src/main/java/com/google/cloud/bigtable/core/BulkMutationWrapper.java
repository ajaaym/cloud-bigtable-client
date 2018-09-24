package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeoutException;

public class BulkMutationWrapper implements BulkMutation{

  private final BulkMutationBatcher bulkMutationBatcher;

  public BulkMutationWrapper(BulkMutationBatcher bulkMutationBatcher) {
    this.bulkMutationBatcher = bulkMutationBatcher;
  }

  @Override public void flush() throws InterruptedException, TimeoutException {
    bulkMutationBatcher.close();
  }

  @Override public void sendUnsent() {

  }

  @Override public boolean isFlushed() {
    return false;
  }

  @Override public ApiFuture<Void> add(RowMutation rowMutation) {
    return bulkMutationBatcher.add(rowMutation);
  }
}

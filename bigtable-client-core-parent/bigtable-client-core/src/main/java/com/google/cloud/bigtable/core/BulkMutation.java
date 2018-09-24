package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface BulkMutation {
  long MAX_RPC_WAIT_TIME_NANOS = TimeUnit.MINUTES.toNanos(12);


  void flush() throws InterruptedException, TimeoutException;

  void sendUnsent();

  boolean isFlushed();

  ApiFuture<Void> add(RowMutation rowMutation);

}

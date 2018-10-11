package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Interface to support batching multiple RowMutation request in to singe grpc request.
 */
public interface BulkMutation {
  long MAX_RPC_WAIT_TIME_NANOS = TimeUnit.MINUTES.toNanos(12);

  /**
   * Send any outstanding {@link MutateRowRequest}s and wait until all requests are complete.
   */
  void flush() throws InterruptedException, TimeoutException;

  void sendUnsent();

  /**
   * @return false if there are any outstanding {@link MutateRowRequest} that still need to be sent.
   */
  boolean isFlushed();

  /**
   * Adds a {@link com.google.cloud.bigtable.data.v2.models.RowMutation} to the underlying BulkMutation
   * mechanism.
   *
   * @param rowMutation The {@link com.google.cloud.bigtable.data.v2.models.RowMutation} to add
   * @return a {@link com.google.common.util.concurrent.SettableFuture} will be set when request is
   * successful otherwise exception will be thrown.
   */
  ApiFuture<Void> add(RowMutation rowMutation);
}

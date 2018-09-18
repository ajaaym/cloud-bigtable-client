package com.google.cloud.bigtable.grpc;

import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.util.concurrent.ExecutionException;

public interface BigtableDataClientCore {
  void mutateRow(RowMutation rowMutation) throws ExecutionException, InterruptedException;
}

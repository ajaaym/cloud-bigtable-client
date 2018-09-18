package com.google.cloud.bigtable.grpc;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class BigtableDataClientCoreImpl implements BigtableDataClientCore{

  private final String clientDefaultAppProfileId;
  private final RequestContext requestContext;
  private final BigtableDataClient bigtableDataClient;

  public BigtableDataClientCoreImpl(BigtableOptions bigtableOptions) throws IOException {
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
}

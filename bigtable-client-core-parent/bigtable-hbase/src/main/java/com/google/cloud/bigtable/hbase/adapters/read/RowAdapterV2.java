package com.google.cloud.bigtable.hbase.adapters.read;


import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.SortedSet;
import java.util.TreeSet;

public class RowAdapterV2 implements ResponseAdapter<Row, Result> {
  @Override
  public Result adaptResponse(Row response) {
    if (response == null) {
      return new Result();
    }

    SortedSet<Cell> hbaseCells = new TreeSet<>(KeyValue.COMPARATOR);
    byte[] rowKey = ByteStringer.extract(response.getKey());
    for(com.google.cloud.bigtable.data.v2.models.RowCell rowCell:response.getCells()) {
      // Cells with labels are for internal use, do not return them.
      // TODO(kevinsi4508): Filter out targeted {@link WhileMatchFilter} labels.
      if(rowCell.getLabels().size()>0) {
        continue;
      }
      byte[] familyNameBytes = Bytes.toBytes(rowCell.getFamily());
      byte[] columnQualifier = ByteStringer.extract(rowCell.getQualifier());
      long hbaseTimestamp = TimestampConverter.bigtable2hbase(rowCell.getTimestamp());
      RowCell keyValue = new RowCell(
          rowKey,
          familyNameBytes,
          columnQualifier,
          hbaseTimestamp,
          ByteStringer.extract(rowCell.getValue()));

      hbaseCells.add(keyValue);
    }

    return Result.create(hbaseCells.toArray(new org.apache.hadoop.hbase.Cell[hbaseCells.size()]));
  }
}

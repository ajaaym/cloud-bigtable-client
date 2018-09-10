package com.google.cloud.bigtable;

import com.google.bigtable.v2.TableName;

import java.util.ArrayList;

public class TestTableNameVsString {



  public static void main(String args[]) {
    long start = System.nanoTime();
    for(int i=0; i<1000000; i++) {
      TableName.of("P1", "instance", "tableId").toString();
    }
    long end = System.nanoTime();
    System.out.println("diff: "+ (end-start));
  }
}

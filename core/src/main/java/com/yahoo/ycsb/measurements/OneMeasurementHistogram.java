/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 *
 */
public class OneMeasurementHistogram extends OneMeasurement {
  public static final String BUCKETS = "histogram.buckets";
  public static final String BUCKETS_DEFAULT = "1000";
  public static final String BUCKET_SIZE = "histogram.bucket.size";
  public static final String BUCKET_SIZE_DEFAULT = "1000";
  public static final String VERBOSE_PROPERTY = "measurement.histogram.verbose";

  /**
   * Specify the range of latencies to track in the histogram.
   */
  private final int buckets;

  /**
   * Specify the side of each bucket in us.
   */
  private final int bucketSize;

  /**
   * Groups operations in discrete blocks of 1ms width.
   */
  private long[] histogram;

  /**
   * Counts all operations outside the histogram's range.
   */
  private long histogramoverflow;

  /**
   * The total number of reported operations.
   */
  private long operations;

  /**
   * The sum of each latency measurement over all operations.
   * Calculated in ms.
   */
  private long totallatency;

  /**
   * The sum of each latency measurement squared over all operations. 
   * Used to calculate variance of latency.
   * Calculated in ms. 
   */
  private double totalsquaredlatency;

  /**
   * Whether or not to emit the histogram buckets.
   */
  private final boolean verbose;
  
  //keep a windowed version of these stats for printing status
  private long windowoperations;
  private long windowtotallatency;

  private int min;
  private int max;

  public OneMeasurementHistogram(String name, Properties props) {
    super(name);
    buckets = Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
    bucketSize = Integer.parseInt(props.getProperty(BUCKET_SIZE, BUCKET_SIZE_DEFAULT));
    verbose = Boolean.valueOf(props.getProperty(VERBOSE_PROPERTY, String.valueOf(false)));
    histogram = new long[buckets];
    histogramoverflow = 0;
    operations = 0;
    totallatency = 0;
    totalsquaredlatency = 0;
    windowoperations = 0;
    windowtotallatency = 0;
    min = -1;
    max = -1;
  }

  /* (non-Javadoc)
   * @see com.yahoo.ycsb.OneMeasurement#measure(int)
   */
  public synchronized void measure(int latency) {
    //latency reported in us and collected in bucket depending on bucketSize.
    if (latency / bucketSize >= buckets) {
      histogramoverflow++;
    } else {
      histogram[latency / bucketSize]++;
    }
    operations++;
    totallatency += latency;
    totalsquaredlatency += ((double) latency) * ((double) latency);
    windowoperations++;
    windowtotallatency += latency;

    if ((min < 0) || (latency < min)) {
      min = latency;
    }

    if ((max < 0) || (latency > max)) {
      max = latency;
    }
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
    double mean = totallatency / ((double) operations);
    double variance = totalsquaredlatency / ((double) operations) - (mean * mean);
    exporter.write(getName(), "Operations", operations);
    exporter.write(getName(), "AverageLatency(us)", mean);
    exporter.write(getName(), "LatencyVariance(us)", variance);
    exporter.write(getName(), "MinLatency(us)", min);
    exporter.write(getName(), "MaxLatency(us)", max);

    long opcounter=0;
    boolean done25th = false;
    boolean done50th = false;
    boolean done60th = false;
    boolean done75th = false;
    boolean done85th = false;
    boolean done95th = false;
    for (int i = 0; i < buckets; i++) {
      opcounter += histogram[i];
      if ((!done25th) && (((double) opcounter) / ((double) operations) >= 0.25)) {
        exporter.write(getName(), "25thPercentileLatency(us)", i * 1000);
        done25th = true;
      }
      if ((!done50th) && (((double) opcounter) / ((double) operations) >= 0.50)) {
        exporter.write(getName(), "50thPercentileLatency(us)", i * 1000);
        done50th = true;
      }
      if ((!done60th) && (((double) opcounter) / ((double) operations) >= 0.60)) {
        exporter.write(getName(), "60thPercentileLatency(us)", i * 1000);
        done60th = true;
      }
      if ((!done75th) && (((double) opcounter) / ((double) operations) >= 0.75)) {
        exporter.write(getName(), "75thPercentileLatency(us)", i * 1000);
        done75th = true;
      }
      if ((!done85th) && (((double) opcounter) / ((double) operations) >= 0.85)) {
        exporter.write(getName(), "85thPercentileLatency(us)", i * 1000);
        done85th = true;
      }
      if ((!done95th) && (((double) opcounter) / ((double) operations) >= 0.95)) {
        exporter.write(getName(), "95thPercentileLatency(us)", i * 1000);
        done95th = true;
      }
      if (((double) opcounter) / ((double) operations) >= 0.99) {
        exporter.write(getName(), "99thPercentileLatency(us)", i * 1000);
        break;
      }
    }

    exportStatusCounts(exporter);

    if (verbose) {
      for (int batchStart = 0; batchStart < buckets; batchStart += 10) {
        long batchSum = 0;
        int batchEnd = Math.min(batchStart + 10, buckets);
        
        for (int i = batchStart; i < batchEnd; i++) {
          batchSum += histogram[i];
        }
        
        if (batchSum > 0) {
          String range;
          if (bucketSize >= 1000) {
            range = (batchStart * bucketSize / 1000) + "-" + ((batchEnd - 1) * bucketSize / 1000) + "ms";
          } else {
            range = (batchStart * bucketSize) + "-" + ((batchEnd - 1) * bucketSize) + "us";
          }
          exporter.write(getName() + "-HISTOGRAM", range, batchSum);
        }
      }
      
      if (bucketSize >= 1000) {
        exporter.write(getName() + "-HISTOGRAM", ">" + buckets * bucketSize / 1000 + "ms", histogramoverflow);
      } else {
        exporter.write(getName() + "-HISTOGRAM", ">" + buckets * bucketSize + "us", histogramoverflow);
      }
    }
  }

  @Override
  public String getSummary() {
    if (windowoperations == 0) {
      return "";
    }
    DecimalFormat d = new DecimalFormat("#.##");
    double report = ((double) windowtotallatency) / ((double) windowoperations);
    windowtotallatency = 0;
    windowoperations = 0;
    return "[" + getName() + " AverageLatency(us)=" + d.format(report) + "]";
  }
}

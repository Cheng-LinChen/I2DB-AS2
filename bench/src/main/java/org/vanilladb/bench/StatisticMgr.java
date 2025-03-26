/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticMgr {
	private static Logger logger = Logger.getLogger(StatisticMgr.class.getName());

	private static class TxnStatistic {
		private BenchTransactionType mType;
		private int txnCount = 0;
		private long totalResponseTimeNs = 0;

		public TxnStatistic(BenchTransactionType txnType) {
			this.mType = txnType;
		}

		public BenchTransactionType getmType() {
			return mType;
		}

		public void addTxnResponseTime(long responseTime) {
			txnCount++;
			totalResponseTimeNs += responseTime;
		}

		public int getTxnCount() {
			return txnCount;
		}

		public long getTotalResponseTime() {
			return totalResponseTimeNs;
		}
	}

	private File outputDir;
	private int timelineGranularity;
	private List<TxnResultSet> resultSets = new ArrayList<>();
	private List<BenchTransactionType> allTxTypes;
	private String fileNamePostfix = "";
	private long recordStartTime = -1;

	public StatisticMgr(Collection<BenchTransactionType> txTypes, File outputDir, int timelineGranularity) {
		this.allTxTypes = new LinkedList<>(txTypes);
		this.outputDir = outputDir;
		this.timelineGranularity = timelineGranularity;
	}

	public StatisticMgr(Collection<BenchTransactionType> txTypes, File outputDir, String namePostfix,
			int timelineGranularity) {
		this.allTxTypes = new LinkedList<>(txTypes);
		this.outputDir = outputDir;
		this.fileNamePostfix = namePostfix;
		this.timelineGranularity = timelineGranularity;
	}

	/**
	 * We use the time that this method is called at as the start time for
	 * recording.
	 */
	public synchronized void setRecordStartTime() {
		if (recordStartTime == -1)
			recordStartTime = System.nanoTime();
	}

	public synchronized void processTxnResult(TxnResultSet trs) {
		if (recordStartTime == -1)
			recordStartTime = trs.getTxnEndTime();
		resultSets.add(trs);
	}

	public synchronized void outputReport() {
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss"); // E.g. "20220324-200824"
			String fileName = formatter.format(Calendar.getInstance().getTime());
			if (fileNamePostfix != null && !fileNamePostfix.isEmpty())
				fileName += "-" + fileNamePostfix; // E.g. "20220324-200824-postfix"

			outputDetailReport(fileName);

			if (fileNamePostfix != null && !fileNamePostfix.isEmpty())
				fileName += "-" + fileNamePostfix + "newReport"; // E.g. "20220324-200824-postfix"

			outputDetailNewReport(fileName);



		} catch (IOException e) {
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Finish creating benchmark report.");
	}

	private void outputDetailNewReport(String fileName) throws IOException {
		final int bucketSizeSec = 5; // Bucket size in seconds
		final long bucketSizeNs = TimeUnit.SECONDS.toNanos(bucketSizeSec); // Convert to nanoseconds

    	Map<BenchTransactionType, Integer> abortedCounts = new HashMap<>();
    
		for (BenchTransactionType type : allTxTypes) {
			abortedCounts.put(type, 0);
		}
	
		// Map for storing transactions grouped by time buckets
		Map<Long, List<Long>> bucketedLatencies = new HashMap<>();
	
		// Iterate over transaction results and classify into time buckets
		for (TxnResultSet resultSet : resultSets) {
			if (!resultSet.isTxnIsCommited()) {
				abortedCounts.put(resultSet.getTxnType(), abortedCounts.get(resultSet.getTxnType()) + 1);
				continue; // Ignore aborted transactions
			}
			long txnEndTime = resultSet.getTxnEndTime();
			long bucketIndex = (txnEndTime - recordStartTime) / bucketSizeNs;
	
			bucketedLatencies.computeIfAbsent(bucketIndex, k -> new ArrayList<>())
							 .add(resultSet.getTxnResponseTime());
		}
	
		// Write report
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputDir, fileName + ".csv")))) {
			writer.write("time(sec), throughput(txs), avg_latency(ms), min(ms), max(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms)");
			writer.newLine();
	
			// Process each bucket
			for (Map.Entry<Long, List<Long>> entry : bucketedLatencies.entrySet()) {
				long timeSec = entry.getKey()* bucketSizeSec;
				List<Long> latencies = entry.getValue();
				
				int throughput = latencies.size();
				long avgLatency = latencies.stream().mapToLong(Long::longValue).sum() / throughput;
				long minLatency = Collections.min(latencies);
				long maxLatency = Collections.max(latencies);
				
				Collections.sort(latencies);
				long p25 = latencies.get(latencies.size() / 4);
				long median = latencies.get(latencies.size() / 2);
				long p75 = latencies.get(3 * latencies.size() / 4);
				
				writer.write(String.format("%d,%d,%d,%d,%d,%d,%d,%d",
					timeSec, throughput, TimeUnit.NANOSECONDS.toMillis(avgLatency),
					TimeUnit.NANOSECONDS.toMillis(minLatency), TimeUnit.NANOSECONDS.toMillis(maxLatency),
					TimeUnit.NANOSECONDS.toMillis(p25), TimeUnit.NANOSECONDS.toMillis(median),
					TimeUnit.NANOSECONDS.toMillis(p75)));
				writer.newLine();
			}
			
			writer.write("# of txns (including aborted) during benchmark period: " + resultSets.size());
			writer.newLine();
			
			int abortedTotal = abortedCounts.values().stream().mapToInt(Integer::intValue).sum();
			int finishedCount = resultSets.size() - abortedTotal;
			double avgResTimeMs = finishedCount > 0 ? resultSets.stream().mapToLong(TxnResultSet::getTxnResponseTime).sum() / finishedCount / 1_000_000.0 : 0;
			writer.write(String.format("TOTAL - committed: %d, aborted: %d, avg latency: %.2f ms", finishedCount, abortedTotal, avgResTimeMs));
		}
	}
	
	
	private void outputDetailReport(String fileName) throws IOException {
		Map<BenchTransactionType, TxnStatistic> txnStatistics = new HashMap<BenchTransactionType, TxnStatistic>();
		Map<BenchTransactionType, Integer> abortedCounts = new HashMap<BenchTransactionType, Integer>();

		for (BenchTransactionType type : allTxTypes) {
			txnStatistics.put(type, new TxnStatistic(type));
			abortedCounts.put(type, 0);
		}

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputDir, fileName + ".txt")))) {
			// First line: total transaction count
			writer.write("# of txns (including aborted) during benchmark period: " + resultSets.size());
			writer.newLine();

			// Detail latency report
			for (TxnResultSet resultSet : resultSets) {
				if (resultSet.isTxnIsCommited()) {
					// Write a line: {[Tx Type]: [Latency]}
					writer.write(resultSet.getTxnType() + ": "
							+ TimeUnit.NANOSECONDS.toMillis(resultSet.getTxnResponseTime()) + " ms");
					writer.newLine();

					// Count transaction for each type
					TxnStatistic txnStatistic = txnStatistics.get(resultSet.getTxnType());
					txnStatistic.addTxnResponseTime(resultSet.getTxnResponseTime());

				} else {
					writer.write(resultSet.getTxnType() + ": ABORTED");
					writer.newLine();

					// Count transaction for each type
					Integer count = abortedCounts.get(resultSet.getTxnType());
					abortedCounts.put(resultSet.getTxnType(), count + 1);
				}
			}
			writer.newLine();

			// Last few lines: show the statistics for each type of transactions
			int abortedTotal = 0;
			for (Map.Entry<BenchTransactionType, TxnStatistic> entry : txnStatistics.entrySet()){
				TxnStatistic value = entry.getValue();
				int abortedCount = abortedCounts.get(entry.getKey());
				abortedTotal += abortedCount;
				long avgResTimeMs = 0;

				if (value.txnCount > 0) {
					avgResTimeMs = TimeUnit.NANOSECONDS.toMillis(value.getTotalResponseTime() / value.txnCount);
				}

				writer.write(value.getmType() + " - committed: " + value.getTxnCount() + ", aborted: " + abortedCount
						+ ", avg latency: " + avgResTimeMs + " ms");

				writer.newLine();
			}

			// Last line: Total statistics
			int finishedCount = resultSets.size() - abortedTotal;
			double avgResTimeMs = 0;
			if (finishedCount > 0) { // Avoid "Divide By Zero"
				for (TxnResultSet rs : resultSets)
					avgResTimeMs += rs.getTxnResponseTime() / finishedCount;
			}
			writer.write(String.format("TOTAL - committed: %d, aborted: %d, avg latency: %d ms", finishedCount,
					abortedTotal, Math.round(avgResTimeMs / 1000000)));
		}
	}

}
	


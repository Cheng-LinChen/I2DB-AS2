package org.vanilladb.bench.benchmarks.as2.rte;

import java.util.ArrayList;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.benchmarks.as2.As2BenchTransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.BenchProperties;
import org.vanilladb.bench.util.RandomValueGenerator;

public class As2UpdateItemParamGen implements TxParamGenerator<As2BenchTransactionType> {
	
	// Read Counts
	private static final int TOTAL_SELECT_COUNT;

	static {
		TOTAL_SELECT_COUNT = BenchProperties.getLoader()
				.getPropertyAsInteger(As2ReadItemParamGen.class.getName() + ".TOTAL_SELECT_COUNT", 10);
	}

	@Override
	public As2BenchTransactionType getTxnType() {
		return As2BenchTransactionType.UPDATE_ITEM;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();
		
		// Set read count
		paramList.add(TOTAL_SELECT_COUNT);
		for (int i = 0; i < TOTAL_SELECT_COUNT; i++){
			paramList.add(rvg.number(1, As2BenchConstants.NUM_ITEMS));
			double randNum = rvg.number(0, 50)/10.0 ; 
			paramList.add(randNum);
		}
		return paramList.toArray(new Object[0]);
	}
}

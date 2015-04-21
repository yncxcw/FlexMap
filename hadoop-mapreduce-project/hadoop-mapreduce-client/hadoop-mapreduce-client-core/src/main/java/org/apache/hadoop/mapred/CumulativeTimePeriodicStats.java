package org.apache.hadoop.mapred;

public class CumulativeTimePeriodicStats extends TimePeriodicStats{

	CumulativeTimePeriodicStats(int count) {
		super(count);
		
		// TODO Auto-generated constructor stub
	}


	protected void extendInternal(int newTime, int newValue) {
		// TODO Auto-generated method stub
		
		 state.currentAccumulation = ((double)(newValue - state.oldValue)/(double)(newTime - state.oldTime));
		
	}

	@Override
	protected void extendInternal(int newTime, double newValue) {
		// TODO Auto-generated method stub
		
		 state.currentAccumulation = ((double)(newValue - state.oldValue)/(double)(newTime - state.oldTime));
		
	}
	
	

}

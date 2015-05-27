package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;

public abstract class TimePeriodicStats {
	
	 // list to store progress speed
	 protected final List<Double> values = new ArrayList<Double>();
	 
	 //time Interval for updating
	 protected final int timeInterval;
	 
	 protected final StatsetState state = new StatsetState();
	 
	 TimePeriodicStats(int interval){
		 
		 this.timeInterval = interval;
		 
	 }
	 
	 
	 protected List<Double> getValues() {
		   
		     return values; 
     }
	    
	  static class StatsetState {
	    double    oldValue = 0;
	    int       oldTime  = 0;
	    double    currentAccumulation = 0;
	  }
	 
	  public double[] toDouble(){
		  
		  if(values.size()==0){
			  			  
			  return new double[0];
		  }
		  
		  double[] Dvalues =new double [values.size()];
		  		  
		  for(int i=0; i<values.size();i++){
			  
			  Dvalues[i] = values.get(i).doubleValue();
		  }
		  
		  return Dvalues;
		  
	  }
	  
	  protected abstract void extendInternal(int newTime, double newValue);
	
	  public void extend(int newTime, double newValue){
		  
		  if(state == null || newTime < state.oldTime){
			  
			  return;
			  
		  }
		  
		  if(newTime - state.oldTime < this.timeInterval){
			  
			 return;
			  
		  }
		  		  		  
		  //compute new speed
		  //double newInsertValue = ((double)(newValue - state.oldValue)/(double)(newTime - state.oldTime));
		  this.extendInternal(newTime, newValue);
		  this.values.add(state.currentAccumulation);
		  
		  //update state struct
		  state.oldTime  = newTime;
		  state.oldValue = newValue;
		  
		  
	  }
	  
	  


}

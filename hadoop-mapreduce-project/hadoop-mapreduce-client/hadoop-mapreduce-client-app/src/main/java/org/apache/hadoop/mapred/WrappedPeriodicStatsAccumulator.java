/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;

//Workaround for PeriodicStateAccumulator being package access
public class WrappedPeriodicStatsAccumulator {

  private PeriodicStatsAccumulator real=null; 
  
  private TimePeriodicStats timeReal=null;
  
  private static final Log LOG = LogFactory.getLog(WrappedPeriodicStatsAccumulator.class);

  public WrappedPeriodicStatsAccumulator(PeriodicStatsAccumulator real) {
	LOG.info("initialize the rael");  
    this.real = real;
  }
  public WrappedPeriodicStatsAccumulator(TimePeriodicStats timeReal) {
	LOG.info("initialize the timeReal");
	this.timeReal = timeReal;
}
 
  public double[] getValue(){
	  
	  if(real!=null){
		  
		 // LOG.info("real is not null and size of real is"+real.toDouble().length);
		  return real.toDouble();
	  }
	  
	  if(timeReal!=null){
		  
		//  LOG.info("timereal is not null and size of real is"+timeReal.toDouble().length);
		  return timeReal.toDouble();
	  }
	  
	//  LOG.info("timereal is null and real is also null");
	  
	  return null;
  }
  
  public void extend(double newParam, double newValue) {
	  
	  
	 if(real !=  null){ 
       real.extend(newParam, (int)newValue);
	   return;
	 }
	 if(timeReal != null){ 
	   timeReal.extend((int)newParam, newValue);
	   return;
	 }
  }
}

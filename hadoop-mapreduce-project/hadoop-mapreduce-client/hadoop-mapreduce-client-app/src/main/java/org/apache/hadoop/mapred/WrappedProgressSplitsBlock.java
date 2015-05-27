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

// Workaround for ProgressSplitBlock being package access
public class WrappedProgressSplitsBlock extends ProgressSplitsBlock {
	
  //for debug
  private static final Log LOG = LogFactory.getLog(WrappedProgressSplitsBlock.class);
	
  private WrappedPeriodicStatsAccumulator wrappedProgressWallclockTime;
  private WrappedPeriodicStatsAccumulator wrappedProgressCPUTime;
  private WrappedPeriodicStatsAccumulator wrappedProgressVirtualMemoryKbytes;
  private WrappedPeriodicStatsAccumulator wrappedProgressPhysicalMemoryKbytes;
  private WrappedPeriodicStatsAccumulator wrappedProgressSpeedTaskAttempt;
  private WrappedPeriodicStatsAccumulator wrappedProgressSpeedHdfsRead;
  private WrappedPeriodicStatsAccumulator wrappedProgressSpeedHdfsWrite;
  private WrappedPeriodicStatsAccumulator wrappedProgressSpeedFileRead;
  private WrappedPeriodicStatsAccumulator wrappedProgressSpeedFileWrite;
  
   
  

  public WrappedProgressSplitsBlock(int numberSplits) {
    super(numberSplits);
  }

  //added by wei for debugging
  public void print(double values[]){
	  
	  for(int i=0;i<values.length;i++){
		  
		  LOG.info(i+"  :  "+values[i]);
	  }
	  
  }
  
  public double[][] burst() {
	  
	//added by wei for debugging  
    LOG.info("wrappedProgressWallclockTime size:"+wrappedProgressWallclockTime.getValue().length);
    print(wrappedProgressWallclockTime.getValue());
        
    LOG.info("wrappedProgressCPUTime size:"+wrappedProgressCPUTime.getValue().length);
    print(wrappedProgressCPUTime.getValue());
    
    LOG.info("wrappedProgressVirtualMemoryKbytes size:"+wrappedProgressVirtualMemoryKbytes.getValue().length);
    print(wrappedProgressVirtualMemoryKbytes.getValue());
    
    LOG.info("wrappedProgressPhysicalMemoryKbytes size:"+wrappedProgressPhysicalMemoryKbytes.getValue().length);
    print(wrappedProgressPhysicalMemoryKbytes.getValue());
    
    LOG.info("wrappedProgressSpeedTaskAttempt size:"+wrappedProgressSpeedTaskAttempt.getValue().length);
    print(wrappedProgressSpeedTaskAttempt.getValue());
    
    LOG.info("wrappedProgressSpeedHdfsRead size:"+wrappedProgressSpeedHdfsRead.getValue().length);
    print(wrappedProgressSpeedHdfsRead.getValue());
    
    LOG.info("wrappedProgressSpeedHdfsWrite size:"+wrappedProgressSpeedHdfsWrite.getValue().length);
    print(wrappedProgressSpeedHdfsWrite.getValue());
    
    LOG.info("wrappedProgressSpeedFileRead size:"+wrappedProgressSpeedFileRead.getValue().length);
    print(wrappedProgressSpeedFileRead.getValue());
    
    LOG.info("wrappedProgressSpeedFileWrite size:"+wrappedProgressSpeedFileWrite.getValue().length);
    print(wrappedProgressSpeedFileWrite.getValue());
    
    double [][] results = new double[9][];

    LOG.info("Start:");
    results[WALLCLOCK_TIME_INDEX] = new double[wrappedProgressWallclockTime.getValue().length];
    for(int i=0;i<wrappedProgressWallclockTime.getValue().length;i++){
    	
    	results[WALLCLOCK_TIME_INDEX][i] = (double)wrappedProgressWallclockTime.getValue()[i];
    }
    LOG.info("finish progressWallclockTime");
    
    results[CPU_TIME_INDEX] = new double[wrappedProgressCPUTime.getValue().length];
    for(int i=0;i<wrappedProgressCPUTime.getValue().length;i++){
    	
    	results[CPU_TIME_INDEX][i] = (double)wrappedProgressCPUTime.getValue()[i];
    } 
    LOG.info("finish progressCPUTime"); 
    
    results[VIRTUAL_MEMORY_KBYTES_INDEX]= new double[wrappedProgressVirtualMemoryKbytes.getValue().length];
    for(int i=0;i<wrappedProgressVirtualMemoryKbytes.getValue().length;i++){
    	
    	results[VIRTUAL_MEMORY_KBYTES_INDEX][i] = (double)wrappedProgressVirtualMemoryKbytes.getValue()[i];
    }
    LOG.info("finish progressVirtualMemoryKbytes");
    
    results[PHYSICAL_MEMORY_KBYTES_INDEX]=new double[wrappedProgressPhysicalMemoryKbytes.getValue().length];
    for(int i=0;i<wrappedProgressPhysicalMemoryKbytes.getValue().length;i++){
    	
    	results[PHYSICAL_MEMORY_KBYTES_INDEX][i] = (double)wrappedProgressPhysicalMemoryKbytes.getValue()[i];
    }
    LOG.info("finish progressPhysicalMemoryKbytes");
    
    //results[WALLCLOCK_TIME_INDEX] = (double [])progressWallclockTime.getValues();
    //results[CPU_TIME_INDEX] = progressCPUTime.getValues();
    //results[VIRTUAL_MEMORY_KBYTES_INDEX] = progressVirtualMemoryKbytes.getValues();
    //results[PHYSICAL_MEMORY_KBYTES_INDEX] = progressPhysicalMemoryKbytes.getValues();
    
    results[PROGRESS_SPEED_TASKATTEMPT_INDEX] = new double[wrappedProgressSpeedTaskAttempt.getValue().length];
    for(int i=0;i<wrappedProgressSpeedTaskAttempt.getValue().length;i++){
      
    	 results[PROGRESS_SPEED_TASKATTEMPT_INDEX][i] = wrappedProgressSpeedTaskAttempt.getValue()[i];
    }
    LOG.info("finish progressSpeedTaskAttempt");
    
    results[PROGRESS_SPEED_DFSREAD_INDEX] = new double[wrappedProgressSpeedHdfsRead.getValue().length];
    for(int i=0;i<wrappedProgressSpeedHdfsRead.getValue().length;i++){
        
   	     results[PROGRESS_SPEED_DFSREAD_INDEX][i] = wrappedProgressSpeedHdfsRead.getValue()[i];
    }
    LOG.info("finish progressSpeedDFSRead");
    
    results[PROGRESS_SPEED_DFSWRITE_INDEX]=new double[wrappedProgressSpeedHdfsWrite.getValue().length];
    for(int i=0;i<wrappedProgressSpeedHdfsWrite.getValue().length;i++){
        
  	     results[PROGRESS_SPEED_DFSWRITE_INDEX][i] = wrappedProgressSpeedHdfsWrite.getValue()[i];
    }
    LOG.info("finish progressSpeedDFSWrite");
    
    results[PROGRESS_SPEED_FILEREAD_INDEX] = new double[wrappedProgressSpeedFileRead.getValue().length];
    for(int i=0;i<wrappedProgressSpeedFileRead.getValue().length;i++){
        
 	     results[PROGRESS_SPEED_FILEREAD_INDEX][i] = wrappedProgressSpeedFileRead.getValue()[i];
    }
    LOG.info("finish progressSpeedFileRead");
    
    results[PROGRESS_SPEED_FILEWRITE_INDEX]= new double[wrappedProgressSpeedFileWrite.getValue().length];
    for(int i=0;i<wrappedProgressSpeedFileWrite.getValue().length;i++){
        
 	     results[PROGRESS_SPEED_FILEWRITE_INDEX][i] = wrappedProgressSpeedFileWrite.getValue()[i];
    }
    LOG.info("finish progressSpeedFileWrite");
    
    return results;
    
  }
  
  public WrappedPeriodicStatsAccumulator getProgressSpeedTaskAttempt(){
	  
	  if(wrappedProgressSpeedTaskAttempt == null){
		  
		  wrappedProgressSpeedTaskAttempt = new WrappedPeriodicStatsAccumulator(
				  this.progressSpeedTaskAttempt);
	  }
	  
	  return wrappedProgressSpeedTaskAttempt;
  }
  
 public WrappedPeriodicStatsAccumulator getProgressSpeedHdfsRead(){
	  
	  if(wrappedProgressSpeedHdfsRead == null){
		  
		  wrappedProgressSpeedHdfsRead = new WrappedPeriodicStatsAccumulator(
				  this.progressSpeedDFSRead);
	  }
	  
	  return wrappedProgressSpeedHdfsRead;
  }

 public WrappedPeriodicStatsAccumulator getProgressSpeedHdfsWrite(){
	  
	  if(wrappedProgressSpeedHdfsWrite == null){
		  
		  wrappedProgressSpeedHdfsWrite = new WrappedPeriodicStatsAccumulator(
				  this.progressSpeedDFSWrite);
	  }
	  
	  return wrappedProgressSpeedHdfsWrite;
 }
 

 public WrappedPeriodicStatsAccumulator getProgressSpeedFileWrite(){
	  
	  if(wrappedProgressSpeedFileWrite == null){
		  
		  wrappedProgressSpeedFileWrite = new WrappedPeriodicStatsAccumulator(
				  this.progressSpeedFileWrite);
	  }
	  
	  return wrappedProgressSpeedFileWrite;
}
 
 
 public WrappedPeriodicStatsAccumulator getProgressSpeedFileRead(){
	  
	  if(wrappedProgressSpeedFileRead == null){
		  
		  wrappedProgressSpeedFileRead = new WrappedPeriodicStatsAccumulator(
				  this.progressSpeedFileRead);
	  }
	  
	  return wrappedProgressSpeedFileRead;
}

 
  public WrappedPeriodicStatsAccumulator getProgressWallclockTime() {
    if (wrappedProgressWallclockTime == null) {
      wrappedProgressWallclockTime = new WrappedPeriodicStatsAccumulator(
          progressWallclockTime);
    }
    return wrappedProgressWallclockTime;
  }

  public WrappedPeriodicStatsAccumulator getProgressCPUTime() {
    if (wrappedProgressCPUTime == null) {
      wrappedProgressCPUTime = new WrappedPeriodicStatsAccumulator(
          progressCPUTime);
    }
    return wrappedProgressCPUTime;
  }

  public WrappedPeriodicStatsAccumulator getProgressVirtualMemoryKbytes() {
    if (wrappedProgressVirtualMemoryKbytes == null) {
      wrappedProgressVirtualMemoryKbytes = new WrappedPeriodicStatsAccumulator(
          progressVirtualMemoryKbytes);
    }
    return wrappedProgressVirtualMemoryKbytes;
  }

  public WrappedPeriodicStatsAccumulator getProgressPhysicalMemoryKbytes() {
    if (wrappedProgressPhysicalMemoryKbytes == null) {
      wrappedProgressPhysicalMemoryKbytes = new WrappedPeriodicStatsAccumulator(
          progressPhysicalMemoryKbytes);
    }
    return wrappedProgressPhysicalMemoryKbytes;
  }
}
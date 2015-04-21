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

// Workaround for ProgressSplitBlock being package access
public class WrappedProgressSplitsBlock extends ProgressSplitsBlock {
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

  public double[][] burst() {
    return super.burst();
  }
  
  public WrappedPeriodicStatsAccumulator getProgressSpeedTaskAttempt(){
	  
	  if(wrappedProgressSpeedTaskAttempt == null){
		  
		  wrappedProgressSpeedTaskAttempt = new WrappedPeriodicStatsAccumulator(
				  progressSpeedTaskAttempt);
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
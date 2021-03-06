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
 
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/*
 * This object gathers the [currently four] PeriodStatset's that we
 * are gathering for a particular task attempt for packaging and
 * handling as a single object.
 */
@Private
@Unstable
public class ProgressSplitsBlock {
  final PeriodicStatsAccumulator progressWallclockTime;
  final PeriodicStatsAccumulator progressCPUTime;
  final PeriodicStatsAccumulator progressVirtualMemoryKbytes;
  final PeriodicStatsAccumulator progressPhysicalMemoryKbytes;
  final TimePeriodicStats        progressSpeedTaskAttempt;
  final TimePeriodicStats        progressSpeedDFSRead;
  final TimePeriodicStats        progressSpeedDFSWrite;
  final TimePeriodicStats        progressSpeedFileRead;
  final TimePeriodicStats        progressSpeedFileWrite;
  
  private static final Log LOG = LogFactory.getLog(ProgressSplitsBlock.class);
  

  static final double[] NULL_ARRAY = new double[0];

  static final int WALLCLOCK_TIME_INDEX = 0;
  static final int CPU_TIME_INDEX = 1;
  static final int VIRTUAL_MEMORY_KBYTES_INDEX = 2;
  static final int PHYSICAL_MEMORY_KBYTES_INDEX = 3;
 
  
  static final int PROGRESS_SPEED_TASKATTEMPT_INDEX = 4;
  
  static final int PROGRESS_SPEED_DFSWRITE_INDEX    = 5;
  
  static final int PROGRESS_SPEED_DFSREAD_INDEX     = 6;
  
  static final int PROGRESS_SPEED_FILEREAD_INDEX    = 7;
  
  static final int PROGRESS_SPEED_FILEWRITE_INDEX   = 8;
  
  static final int DEFAULT_TIME_INTERVAL = 1*1000; 

  ProgressSplitsBlock(int numberSplits) {
    progressWallclockTime
      = new StatePeriodicStats(numberSplits);
    progressCPUTime
      = new StatePeriodicStats(numberSplits);
    progressVirtualMemoryKbytes
      = new StatePeriodicStats(numberSplits);
    progressPhysicalMemoryKbytes
      = new StatePeriodicStats(numberSplits);
    
    progressSpeedTaskAttempt 
        = new CumulativeTimePeriodicStats(DEFAULT_TIME_INTERVAL);
    progressSpeedDFSRead        
        = new CumulativeTimePeriodicStats(DEFAULT_TIME_INTERVAL);
    progressSpeedDFSWrite
        = new CumulativeTimePeriodicStats(DEFAULT_TIME_INTERVAL);
    progressSpeedFileRead
        = new CumulativeTimePeriodicStats(DEFAULT_TIME_INTERVAL);
    progressSpeedFileWrite
        = new CumulativeTimePeriodicStats(DEFAULT_TIME_INTERVAL);
  }

  // this coordinates with LoggedTaskAttempt.SplitVectorKind
  double[][] burst() {
	   
    return null;
  }

  static public double[] arrayGet(double[][] burstedBlock, int index) {
    return (burstedBlock == null ? NULL_ARRAY : burstedBlock[index]);
  }

  static public double[] arrayGetWallclockTime(double[][] burstedBlock) {
    return arrayGet(burstedBlock, WALLCLOCK_TIME_INDEX);
  }

  static public double[] arrayGetCPUTime(double[][] burstedBlock) {
    return arrayGet(burstedBlock, CPU_TIME_INDEX);
  }

  static public double[] arrayGetVMemKbytes(double[][] burstedBlock) {
    return arrayGet(burstedBlock, VIRTUAL_MEMORY_KBYTES_INDEX);
  }

  static public double[] arrayGetPhysMemKbytes(double[][] burstedBlock) {
    return arrayGet(burstedBlock, PHYSICAL_MEMORY_KBYTES_INDEX);
  }
  
  static public double[] arrayGetProgressSpeedTaskAttempt(double[][] burstedBlock) {
	    return arrayGet(burstedBlock, PROGRESS_SPEED_TASKATTEMPT_INDEX);
  }
  
  static public double[] arrayGetProgressSpeedDFSWrite(double[][] burstedBlock) {
	    return arrayGet(burstedBlock, PROGRESS_SPEED_DFSWRITE_INDEX);
}
  
  static public double[] arrayGetProgressSpeedDFSRead(double[][] burstedBlock) {
	    return arrayGet(burstedBlock, PROGRESS_SPEED_DFSREAD_INDEX);
}
  
  static public double[] arrayGetProgressSpeedFileWrite(double[][] burstedBlock) {
	    return arrayGet(burstedBlock, PROGRESS_SPEED_FILEWRITE_INDEX);
}
  
  static public double[] arrayGetProgressSpeedFileRead(double[][] burstedBlock) {
	    return arrayGet(burstedBlock, PROGRESS_SPEED_FILEREAD_INDEX);
}
  
}
    

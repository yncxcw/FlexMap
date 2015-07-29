package org.apache.hadoop.mapreduce.v2.app.job.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiMapTaskAttemptImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

@SuppressWarnings({ "rawtypes" })

public class MultiMapTaskImpl extends TaskImpl{
		 
	 private final String[] nodeList;  	
 	
	 public MultiMapTaskImpl(JobId jobId, int partition, EventHandler eventHandler,
			      Path remoteJobConfFile, JobConf conf,
			      String[] nodeList,
			      TaskAttemptListener taskAttemptListener,
			      Token<JobTokenIdentifier> jobToken,
			      Credentials credentials, Clock clock,
			      int appAttemptId, MRAppMetrics metrics, AppContext appContext) {
			    super(jobId, TaskType.MAP, partition, eventHandler, remoteJobConfFile,
			        conf, taskAttemptListener, jobToken, credentials, clock,
			        appAttemptId, metrics, appContext);
			    this.nodeList = nodeList;    
	    }

		  @Override
	  protected int getMaxAttempts() {
		    
			  return conf.getInt(MRJobConfig.MAP_MAX_ATTEMPTS, 4);
	
	  }

		  @Override
	  protected TaskAttemptImpl createAttempt() {
			
			 //I should add configure jude here
			  
		    return new MultiMapTaskAttemptImpl(getID(), nextAttemptNumber,
		        eventHandler, taskAttemptListener,jobFile,partition,conf,nodeList,
		        jobToken, credentials, clock, appContext);
      }

		  @Override
	  public TaskType getType() {
		    return TaskType.MAP;
	  }

	 
		  /**
		   * @return a String formatted as a comma-separated list of splits.
		   */
		  @Override
		  protected String getSplitsAsString() {
			 
			/*  
		    String[] splits = getTaskSplitMetaInfo().getLocations();
		    if (splits == null || splits.length == 0)
		    return "";
		    StringBuilder sb = new StringBuilder();
		    for (int i = 0; i < splits.length; i++) {
		      if (i != 0) sb.append(",");
		      sb.append(splits[i]);
		    }
		    return sb.toString();
		    */	  
		   return "multisplit";
		  }

	
}

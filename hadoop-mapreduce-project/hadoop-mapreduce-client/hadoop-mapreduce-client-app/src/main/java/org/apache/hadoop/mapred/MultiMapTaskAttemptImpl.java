package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class MultiMapTaskAttemptImpl extends TaskAttemptImpl {

	TaskSplitMetaInfo[] splitInfos;
	
	static final Log LOG = LogFactory.getLog(TaskAttemptImpl.class);
	
	public MultiMapTaskAttemptImpl(TaskId taskId, int i,
			EventHandler eventHandler, TaskAttemptListener taskAttemptListener,
			Path jobFile, int partition, JobConf conf, String[] dataLocalHosts,
			Token<JobTokenIdentifier> jobToken, Credentials credentials,
			Clock clock, AppContext appContext) {
		super(taskId, i, eventHandler, taskAttemptListener, jobFile, partition, conf,
				dataLocalHosts, jobToken, credentials, clock, appContext);
				
		// TODO Auto-generated constructor stub
	}
	


	public void setSplitInfos(TaskSplitMetaInfo[] splitInfos){
		
		LOG.info("set splitInfo length: "+splitInfos.length);
		
		this.splitInfos=splitInfos;
	}
	
	public TaskSplitMetaInfo[] getTaskSplitMetaInfo(){
		
		return this.splitInfos;
		
	}

	@Override
	protected Task createRemoteTask() {
		 TaskSplitIndex splitIndex[] = new TaskSplitIndex[splitInfos.length];
		 int i=0;
		 for(TaskSplitMetaInfo splitInfo:splitInfos){
			 
			 splitIndex[i] = splitInfo.getSplitIndex();
			 
			 i++;
		 }
		 MapTask mapTask =
			      new MultiMapTask("", TypeConverter.fromYarn(getID()), partition,splitIndex, 1); // YARN doesn't have the concept of slots per task, set it as 1.
				  //new MultiMapTask();
			    mapTask.setUser(conf.get(MRJobConfig.USER_NAME));
			    mapTask.setConf(conf);
			    mapTask.setTaskType(TaskType.MULTI_MAP);
			    
    return mapTask;

	}
	
	

}

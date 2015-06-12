package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.JobContext;

public class DefaultTaskDataProvision extends TaskDataProvision {

	public DefaultTaskDataProvision(JobContext jobContext) {
		super(jobContext);
		// TODO Auto-generated constructor stub
	}
	
	public DefaultTaskDataProvision(){
		
		super();
	}

	@Override
	public TaskSplitMetaInfo[] getNewSplitOnNode(String node, int blockNum) {
		// TODO Auto-generated method stub
		return null;
	}

}

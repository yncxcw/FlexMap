package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.Container;

public class JobTaskAttemptContainerAssinged extends JobEvent {
	
	TaskAttemptId map;
	Container container;

	public JobTaskAttemptContainerAssinged(TaskAttemptId map, Container container) {
		super(map.getTaskId().getJobId(), JobEventType.JOB_TASK_ATTEMPT_CONTAINER_ASSIGNED);
		// TODO Auto-generated constructor stub
		
		this.map = map;
		this.container= container;
		
	}
	
	public TaskAttemptId getTaskAttemptID() {
	
		return map;
	}
	
	public Container getContainer(){
		
		return container;
	}

}

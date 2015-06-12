package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.split.*;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class TaskDataProvision {
	
	static final Log LOG = LogFactory.getLog(TaskDataProvision.class);
	
	Configuration jobConf;
	
	LinkedList<SplitInfo> assignedSplits;            //block that has been assigned to one node to execute
	
	Map<String,Long>  assignedSize;                  //map from each node to processed size
	
	Map<String,Set<SplitInfo>> nodeToSplits;         //map from each node to unprocessed block 
	
	Map<String,Set<SplitInfo>> rackToSplits;         //map from each rack to unprocessed block
	
	Map<SplitInfo,String[]> splitToNode;             //map from each block to each node
	
	TaskSplitMetaInfo TASK_SPPLIT_META_INFO_NULL[];
	
	public TaskDataProvision(){
		
        assignedSplits=new LinkedList<SplitInfo>();
		
		assignedSize  =new HashMap<String,Long>();
		
		nodeToSplits  =new HashMap<String,Set<SplitInfo>>();
		
		rackToSplits  =new HashMap<String,Set<SplitInfo>>();
		
		splitToNode   =new HashMap<SplitInfo,String[]>();
		
	}
	
	public TaskDataProvision(JobContext jobContext){
		
		jobConf=jobContext.getConfiguration();
		
		assignedSplits=new LinkedList<SplitInfo>();
		
		assignedSize  =new HashMap<String,Long>();
		
		nodeToSplits  =new HashMap<String,Set<SplitInfo>>();
		
		rackToSplits  =new HashMap<String,Set<SplitInfo>>();
		
		splitToNode   =new HashMap<SplitInfo,String[]>();
	}
	
	/**
	 * initialize data structure to make map from block to node 
	 * and to make node to block, these struct know how much data has been allocated to one node
	 */
	public void initialize(TaskSplitMetaInfo[] taskSplitMetaInfos){
		
		//if we already have some elements , just get rid of it.
		
		if(assignedSplits.size() > 0 ){      
			
			assignedSplits.clear();
		}
	
		if(nodeToSplits.size()>0){
			
			nodeToSplits.clear();
		}
		
		if(rackToSplits.size()>0){
			
			rackToSplits.clear();
		}
		
		if(splitToNode.size()>0){
		
		    splitToNode.clear();
		}
		
		//iterate through each taskSplitMetaInfo
		
		for(TaskSplitMetaInfo taskSplitMetaInfo: taskSplitMetaInfos){
			
			SplitInfo splitInfo =new SplitInfo(taskSplitMetaInfo,null);
			
			splitToNode.put(splitInfo, splitInfo.hosts);
			
			//add this node to host to split map
			
			for(String host : splitInfo.hosts){
			
			Set<SplitInfo> splitSet=nodeToSplits.get(host);
			
			if(splitSet==null){
				
				splitSet = new LinkedHashSet<SplitInfo>();
				nodeToSplits.put(host, splitSet);
			}
			splitSet.add(splitInfo);
			
			}
			
			//add this node to rack to split map
			for(String rack : splitInfo.racks){
				
				Set<SplitInfo> splitSet=rackToSplits.get(rack);
				
				if(splitSet==null){
					
					splitSet = new LinkedHashSet<SplitInfo>();
					rackToSplits.put(rack, splitSet);
				}
				splitSet.add(splitInfo);
				
				}
			
		}
		
		LOG.info("after initializing");
		for (Map.Entry<String,Set<SplitInfo>> entry : nodeToSplits.entrySet()) {
		    String keys = entry.getKey();
		    Set<SplitInfo> values = entry.getValue();
		    
		    LOG.info("node "+keys);
		    
		    for(SplitInfo value:values){
		    	
		    	LOG.info("split:"+value.toString());
		    }
		    
		    
		    
		    // ...
		}
				
	}
	
	
	public synchronized void addMoreSplits(TaskSplitMetaInfo[] taskSplitMetaInfos){
		
       for(TaskSplitMetaInfo taskSplitMetaInfo: taskSplitMetaInfos){
			
			SplitInfo splitInfo =new SplitInfo(taskSplitMetaInfo,null);
			
			splitToNode.put(splitInfo, splitInfo.hosts);
			
			//add this node to host to split map
			
			for(String host : splitInfo.hosts){
			
			Set<SplitInfo> splitSet=nodeToSplits.get(host);
			
			if(splitSet==null){
				
				splitSet = new LinkedHashSet<SplitInfo>();
				nodeToSplits.put(host, splitSet);
			}
			splitSet.add(splitInfo);
			
			}
			
			//add this node to rack to split map
			for(String rack : splitInfo.racks){
				
				Set<SplitInfo> splitSet=rackToSplits.get(rack);
				
				if(splitSet==null){
					
					splitSet = new LinkedHashSet<SplitInfo>();
					rackToSplits.put(rack, splitSet);
				}
				splitSet.add(splitInfo);
				
				}
			
		}
		
	}
	
	//this functon must be synchronized
	public abstract TaskSplitMetaInfo[] getNewSplitOnNode(String node,int blockNum);
	
	public synchronized TaskSplitMetaInfo[] getAllSplitOnNode(String node){
	 
		ArrayList<TaskSplitMetaInfo> taskSplitMetaInfos=new ArrayList<TaskSplitMetaInfo>();
		
		LOG.info("get all splits on node"+node);
		
		if(isSplitAllocationDone()){
			
			LOG.info("ndoe to split is null");
			return TASK_SPPLIT_META_INFO_NULL;
		}
		
		Set<SplitInfo> splitInfos = nodeToSplits.get(node);
		
		if(splitInfos.isEmpty()){
			
			LOG.info("ndoe"+node+"is null");
			return TASK_SPPLIT_META_INFO_NULL;
		}
		
		LOG.info("ndoe"+node+"size is"+splitInfos.size());
		
		Iterator<SplitInfo> iterator=splitInfos.iterator();
		
		while(iterator.hasNext()){
			
			SplitInfo splitInfo = iterator.next();
			
			LOG.info("splitinfo:"+splitInfo.toString());
			
			taskSplitMetaInfos.add(splitInfo.taskSplitMetaInfo);
			
			assignedSplits.add(splitInfo);
			
			Long nodeSize = assignedSize.get(node);
			
			if(nodeSize==null){
				
				 nodeSize = new Long(splitInfo.length);
				
			}else{
			
			     nodeSize =new Long(nodeSize.longValue()+splitInfo.length);
			
			}
			 assignedSize.put(node,nodeSize);
			 
			 iterator.remove();
		}
		
		
		return taskSplitMetaInfos.toArray(new TaskSplitMetaInfo[taskSplitMetaInfos.size()]);
						
	}
	
	public synchronized int  getSplitsDataNode(){
		
		 return nodeToSplits.keySet().size();
	}
	
	public synchronized String[] getSplitsNodeList(){
		
		return nodeToSplits.keySet().toArray(new String[nodeToSplits.keySet().size()]);
	}
	
	private boolean isSplitAllocationDone(){
		
		if(nodeToSplits.isEmpty()){
			
			return true;
		}else{
			
			return false;
		}
				
	}
	
	public static class SplitInfo{	
		TaskSplitMetaInfo taskSplitMetaInfo;    //split metainfo fot this split  
		long length;                            //length of this block
		String[] hosts;                         //nodes on which this block resides
		String[] racks;                         //racks on which this block resides
		
		SplitInfo(TaskSplitMetaInfo taskSplitMetaInfo,String []topologyPath){
				
			this.taskSplitMetaInfo = taskSplitMetaInfo;
			this.length = taskSplitMetaInfo.getInputDataLength();
			this.hosts  = taskSplitMetaInfo.getLocations();
			
			assert(hosts.length==topologyPath.length||topologyPath.length==0);
			
			//if this fs does not have any rack information,use default rack
			if(topologyPath==null||topologyPath.length==0){
			  topologyPath = new String[hosts.length];	
			 	for(int i=0;i<hosts.length;i++){
			 		topologyPath[i]=(new NodeBase(hosts[i],NetworkTopology.DEFAULT_RACK)).toString();
			 	}	
			}
			
			//the topology pahts have the host name as the last component,strip it
			this.racks = new String[hosts.length];
			for(int i=0;i<racks.length;i++){
				
				this.racks[i]=(new NodeBase(topologyPath[i])).getNetworkLocation();
			}
			
		}
		
	}
	

	
}

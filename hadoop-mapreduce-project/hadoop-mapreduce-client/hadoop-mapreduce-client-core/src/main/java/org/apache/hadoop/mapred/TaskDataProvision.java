package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.split.*;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class TaskDataProvision {
	
	static final Log LOG = LogFactory.getLog(TaskDataProvision.class);
	
	protected int BLOCKS_UNIT;
	
	protected double BLOCKS_UNIT_FAST_LIMIT = 0.8;
	
	protected double BLOCKS_UNIT_LIMIT = 0.9;
	
	protected long MAX_SPEED_RATIO = 10;
	
	protected long LOCAL_DATA = 0;
	
	protected long SPEED_QUEUE_LENGTH = 10;
	
	protected long UPPER_BLOCK_UNIT_COMBINATION = 20;
	
	Configuration jobConf;
	
	protected Map<String,Long> nodeToRound;          //record each round of wave
	
	protected Map<String,Long> nodeToLastRunTime;    //record last run time for this machine
	
	protected Map<String,Long> nodeToBlockUnit;      //map to node to block unit # on this node
	
	protected Map<String,Double> nodeToRatio;        //map from each node to ratio
 	
	protected long ROUND = 8;                        //how many round to increase the BLOCKS_UNIT
	
	LinkedList<SplitInfo> assignedSplits;            //block that has been assigned to one node to execute
	
	Map<String,Long>  assignedSize;                  //map from each node to processed size
	
	Map<String,Set<SplitInfo>> nodeToSplits;         //map from each node to unprocessed block 
	
	Map<String,Set<SplitInfo>> rackToSplits;         //map from each rack to unprocessed block
	
	Map<SplitInfo,String[]> splitToNode;             //map from each block to each node
	
	Map<String, LinkedList<Long>> nodeToSpeedQueue;  //map from each node to Queue speed of that node
	
	Map<String,Long> nodeToSpeed;                    //map from each node to block number
	
	Map<String,Long> nodeToFinishSpeed;              //map from each node to the map finished speed
	
	Map<String,Long>nodeToBlockNum;                  //currently assigned block num for each node
	
    
	double totalData = 0;
	
	private final Map<String, Set<TaskSplitMetaInfo>>  TaskIdToSplitInfo = new HashMap<String,Set<TaskSplitMetaInfo>>();
	
	TaskSplitMetaInfo TASK_SPPLIT_META_INFO_NULL[];
	
	public TaskDataProvision(){
		
		BLOCKS_UNIT      = 2;
        assignedSplits   = new LinkedList<SplitInfo>();
		assignedSize     = new HashMap<String,Long>();
		nodeToSplits     = new HashMap<String,Set<SplitInfo>>();
		rackToSplits     = new HashMap<String,Set<SplitInfo>>();
		splitToNode      = new HashMap<SplitInfo,String[]>();
		nodeToSpeed      = new LinkedHashMap<String,Long>();
		nodeToRatio      = new LinkedHashMap<String,Double>();
		nodeToSpeedQueue = new HashMap<String,LinkedList<Long>>();
		nodeToRound      = new HashMap<String,Long>();
		nodeToBlockUnit  = new HashMap<String,Long>();
		nodeToLastRunTime= new HashMap<String,Long>();
		nodeToBlockNum   = new HashMap<String,Long>();
		nodeToFinishSpeed= new HashMap<String,Long>();
 		
	 
		
	}
	
	public TaskDataProvision(JobContext jobContext){
		
		jobConf=jobContext.getConfiguration();
		BLOCKS_UNIT = jobConf.getInt(MRJobConfig.BOCK_UNIT_UNIT, 1);
		BLOCKS_UNIT_LIMIT=jobConf.getDouble(MRJobConfig.BLOCKS_UNIT_LIMIT, 0.9);
		BLOCKS_UNIT_FAST_LIMIT=jobConf.getDouble(MRJobConfig.BLOCKS_UNIT_FAST_LIMIT, 0.8);
				
		assignedSplits   = new LinkedList<SplitInfo>();
		assignedSize     = new HashMap<String,Long>();
		nodeToSplits     = new HashMap<String,Set<SplitInfo>>();
		rackToSplits     = new HashMap<String,Set<SplitInfo>>();
		splitToNode      = new HashMap<SplitInfo,String[]>();
		nodeToSpeed      = new LinkedHashMap<String,Long>();
		nodeToRatio      = new LinkedHashMap<String,Double>();
		nodeToSpeedQueue = new HashMap<String,LinkedList<Long>>();
		nodeToRound      = new HashMap<String,Long>();
		nodeToBlockUnit  = new HashMap<String,Long>();
		nodeToLastRunTime= new HashMap<String,Long>();
		nodeToBlockNum   = new HashMap<String,Long>();
		nodeToFinishSpeed= new HashMap<String,Long>();
	}
	
	
	public boolean updateLastRoundTime(String node){  // return if it's a new round of wave
		
		long now = System.currentTimeMillis();
		boolean newRound;
		
		if(this.nodeToLastRunTime.get(node)!=null){
			
		LOG.info("lastRoundTimegap:"+(now - nodeToLastRunTime.get(node)));
			
		if((double)(now - nodeToLastRunTime.get(node))/1000 > 3.0){
			
			newRound = true;
		
		}else{
			
		    newRound = false;	
		
		}
	
	}else{
		
		newRound = false;
	}
		
		this.nodeToLastRunTime.put(node,now);
		
		return newRound;
	}
	
	public void updateRound(String node){
		
		this.nodeToRound.put(node, nodeToRound.get(node)+1);
	}
	
	public long getRound(String node){
		
		return this.nodeToRound.get(node);
	}
	
	private synchronized void sortSpeedByComparator(){
		
		List<Map.Entry<String,Long>> entryList = new LinkedList<Map.Entry<String,Long>>(nodeToSpeed.entrySet());
		
		Collections.sort(entryList, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Map.Entry<String, Long> o1,
                                           Map.Entry<String, Long> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});
		
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
			for (Iterator<Map.Entry<String, Long>> it = entryList.iterator(); it.hasNext();) {
				Map.Entry<String, Long> entry = it.next();
				sortedMap.put(entry.getKey(), entry.getValue());
			}
			
	   nodeToSpeed = sortedMap;		
					
	}
	
	public synchronized void updateNodeSpeed(String node, long speed, double ratio, boolean isTaskFinished){
		
		
		if(nodeToSpeedQueue.get(node).size() == 0){                    //first update
			
			nodeToFinishSpeed.put(node, speed);
		}
		
		
		//----------update speed---------------------------------------
		if(nodeToSpeedQueue.get(node).size() >= SPEED_QUEUE_LENGTH){
		
			 nodeToSpeedQueue.get(node).remove();
		
		}
		
		nodeToSpeedQueue.get(node).add(speed);
		
		double up   = 0;
		double down = 0;
		
		for(int i=1;i <= nodeToSpeedQueue.get(node).size();i++){
			
			up   = up + i*nodeToSpeedQueue.get(node).get(i-1);
			down = down + i;
			
		}
		
		
		long unitSpeed;
		
		if(isTaskFinished){
		   
			LOG.info("task finished event");
			
			unitSpeed= speed;
			
			nodeToFinishSpeed.put(node, speed);
		
		}else{
			//if(nodeToFinishSpeed.get(node) > 0){                  //we have already got first finished task
		      unitSpeed= (long)((double)(up/down)*0.2+nodeToFinishSpeed.get(node)*0.8);
			//}else{                                                //we use the first finished task
				
			//  unitSpeed= (long)(up/down);	
			//}
		}
		
		
		
		
		nodeToSpeed.put(node, unitSpeed);
		
		LOG.info("nodeSpeed");
		if(isTaskFinished){
		  LOG.info("finishedSpeed:"+speed);	
		}else{
		  LOG.info("finishedSpeed:"+0);	 	
		}
		LOG.info("speed"+":"+nodeToSpeed.get(node));
		LOG.info("realSpeed"+":"+speed);
		LOG.info("node"+":"+node);
		LOG.info("/nodeSpeed");
		
		//-----------------update speed ratio----------
		
		if(ratio<=0){
			
			
			return;
		}
		
		double newRatio;
		
		if(this.nodeToRatio.get(node) <=0 ){
			
			this.nodeToRatio.put(node, ratio);
		
		}else{
			
			up    = 1*this.nodeToRatio.get(node)+4*ratio;
			down  = 5.0;
			newRatio = up/down;
		    this.nodeToRatio.put(node, newRatio);
			
		}
	
		
	}
	
	
	public String getSlowestNode(){
		
	this.sortSpeedByComparator();                //sort
	
	String[]  nodeArray   = nodeToSpeed.keySet().toArray(new String[nodeToSpeed.size()]);
    Long[]    speedArray  = nodeToSpeed.values().toArray(new Long[nodeToSpeed.size()]);
    
    int i=0;
    
    while(i < speedArray.length){
    	
    	if(speedArray[i] !=0){
    		
    		break;
    	}
    	
    	i++;
    }
    
    if(i>=speedArray.length){
    	
    	i=0;
    }
       
	return nodeArray[i];			
}
	
	
	public String getFastestNode(){
		
	this.sortSpeedByComparator();    //sort
		
    String[]  nodeArray   = nodeToSpeed.keySet().toArray(new String[nodeToSpeed.size()]);
	Long[] speedArray     = nodeToSpeed.values().toArray(new Long[nodeToSpeed.size()]);
	    
    return nodeArray[nodeArray.length-1];		
		
	}
	
	public List<String> getFastestNNode(int N){
		
	this.sortSpeedByComparator();               //sort
		
	String[]  nodeArray   = nodeToSpeed.keySet().toArray(new String[nodeToSpeed.size()]);
	Long[]    speedArray     = nodeToSpeed.values().toArray(new Long[nodeToSpeed.size()]);
	
	List<String> result = new ArrayList<String>();
	
	for(int i=0;i<N && i<nodeArray.length;i++){
		
	  result.add(nodeArray[N-1-i]);	
		
	}
			
	return result;	
	} 
	
	
	public List<String> getSortedNodeList(){
		
		return getFastestNNode(this.nodeToSpeed.size());
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
		
		if(nodeToSpeed.size()>0){
			
			nodeToSpeed.clear();
		}
		
		if(nodeToSpeedQueue.size()>0){
			
			nodeToSpeed.clear();
		}
		
		if(nodeToBlockNum.size()>0){
			
			nodeToBlockNum.clear();
		}
		
		if(nodeToRatio.size()>0){
			
			nodeToRatio.clear();
		}
		
		if(nodeToBlockUnit.size() > 0){
			
			nodeToBlockUnit.clear();
		}
		
		if(nodeToFinishSpeed.size() > 0){
			
			nodeToFinishSpeed.clear();
		}
		
		//iterate through each taskSplitMetaInfo
		
		for(TaskSplitMetaInfo taskSplitMetaInfo: taskSplitMetaInfos){
			
			totalData += taskSplitMetaInfo.getInputDataLength();
			
			SplitInfo splitInfo =new SplitInfo(taskSplitMetaInfo,null);
			
			splitToNode.put(splitInfo, splitInfo.hosts);
			
			//add this node to host to split map
			
			for(String host : splitInfo.hosts){	
		    
			nodeToSpeed.put(host, (long)0);
			nodeToFinishSpeed.put(host, (long)0);
			nodeToRatio.put(host, (double)0);
			nodeToRound.put(host, (long)0);
			nodeToBlockUnit.put(host, (long)BLOCKS_UNIT);
		    nodeToBlockNum.put(host, (long)1);
		    
		    
		    LinkedList<Long> speedQueue = new LinkedList<Long>();
		    nodeToSpeedQueue.put(host, speedQueue);
		    
		    
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
		
		
		
		LOG.info("after initializing,block unit is   "+this.BLOCKS_UNIT);
						
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

	public synchronized void regiserForTask(String taskId, Set<TaskSplitMetaInfo> taskSplitMetaInfo){
	
		this.TaskIdToSplitInfo.put(taskId, taskSplitMetaInfo);
	}

	public long getAssignedDataSize(){
		
		long dataSize = 0;
		
		for(Set<TaskSplitMetaInfo> taskSplitMetaInfoSet : this.TaskIdToSplitInfo.values()){
			
			for(TaskSplitMetaInfo taskSplitMetaInfo:taskSplitMetaInfoSet){
				
				dataSize += taskSplitMetaInfo.getInputDataLength();
			}
			
		}
		
		return dataSize;
		
	}
	public int getTotalBlocksNum(){
		
		if(this.splitToNode!=null){
			
			return splitToNode.size();
		}
		
		return 0;	
	}
	
	public Set<TaskSplitMetaInfo> getSplitsForTask(String taskId){
		
		if(!this.TaskIdToSplitInfo.containsKey(taskId)){
			
			return null;
		}
		
		return this.TaskIdToSplitInfo.get(taskId);
	}
	
	//this functon must be synchronized
	public abstract Set<TaskSplitMetaInfo> getNewSplitOnNode(String node,int blockNum,boolean locality);
	
	public abstract Set<TaskSplitMetaInfo> getAutomatedSplitOnNode(String node,boolean locality);
	
	 /*  return blockNumber of blocks from node, blockNumber > node.blockNumber then just return all the blocks on this node
	  * 
	  */
	public synchronized Set<TaskSplitMetaInfo> getBlocksOnNode(String node, int blockNumber){
		
     Set<TaskSplitMetaInfo> taskSplitMetaInfos=new LinkedHashSet<TaskSplitMetaInfo>();
     
     Set<SplitInfo> splitInfos = nodeToSplits.get(node);
      
	 if(isSplitAllocationDone()){
		 	
		LOG.info("can not splitted again");
		LOG.info("end total assigned: "+ this.getAssignedDataSize());
		return taskSplitMetaInfos;               // can not be null here
	 }
		
		
	 LOG.info("data size on each node");
	 long totalSize = 0;
		for(Map.Entry<String, Long> pairs : this.nodeToBlockNum.entrySet()){
			
			LOG.info("data    "+pairs.getKey()+"size    "+pairs.getValue());
			
			totalSize+=pairs.getValue();
			
	 }
	LOG.info("total size:"+totalSize);
	
	if(splitInfos.isEmpty() || splitInfos==null){		
		
	     LOG.info("ndoe is null:  "+node);
		return taskSplitMetaInfos;
    }
	
	
		Iterator<SplitInfo> iterator=splitInfos.iterator();
		
		int number = 0;
		
		while(iterator.hasNext()){                            
			
			SplitInfo splitInfo = iterator.next();
			
			taskSplitMetaInfos.add(splitInfo.taskSplitMetaInfo);
			
			assignedSplits.add(splitInfo);
			
			Long nodeSize = assignedSize.get(node);
			
			if(nodeSize==null){
				
				 nodeSize = new Long(splitInfo.length);
				
			}else{
			
			     nodeSize =new Long(nodeSize.longValue()+splitInfo.length);
			
			}
			 assignedSize.put(node,nodeSize);
			 
			 iterator.remove();             //delete this split from this node
			 
			 deleteSplitInfo(splitInfo);     //delete this split from other nodes and racks
			 
			 if(++number >= blockNumber){
				 
				 break;
			 }
		}
			
		return taskSplitMetaInfos;
		
		
	}
	
	public Set<TaskSplitMetaInfo> getOneSplitOnNode(String node){
		
		return getBlocksOnNode(node,1);
	}
	
	public synchronized Set<TaskSplitMetaInfo> getAllSplitOnNode(String node){
	 
		Set<SplitInfo> splitInfos = nodeToSplits.get(node);
	    
		int size = splitInfos.size();
	
		return getBlocksOnNode(node,size);
		
	}

	public synchronized void deleteSplitInfo(SplitInfo splitInfo){
           
          Set<String> nodeSet = nodeToSplits.keySet();
          Set<String> rackSet = rackToSplits.keySet();
          
          for(String nodeStr : nodeSet){
        	  
        	  this.nodeToSplits.get(nodeStr).remove(splitInfo);
        	  
          }
          
          for(String rackStr : rackSet){
        	  
        	  this.rackToSplits.get(rackStr).remove(splitInfo);
        	  
          }

        }
	
	public synchronized int  getSplitsDataNode(){
		
		 return nodeToSplits.keySet().size();
	}
	
	public synchronized String[] getSplitsNodeList(){
		
		return nodeToSplits.keySet().toArray(new String[nodeToSplits.keySet().size()]);
	}
	
	protected boolean isSplitAllocationDone(){
		
		for(String node : this.nodeToSplits.keySet()){
			
			if(nodeToSplits.get(node).size() > 0){
				
				return false;
			}
		}
			
	   return true;		
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

package org.apache.hadoop.mapred;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapred.TaskDataProvision.SplitInfo;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultTaskDataProvision extends TaskDataProvision {
	
	static final Log LOG = LogFactory.getLog(TaskDataProvision.class);

	public DefaultTaskDataProvision(JobContext jobContext) {
		super(jobContext);
		// TODO Auto-generated constructor stub
	}
	
	public DefaultTaskDataProvision(){
		
		super();
	}

	@Override
	public synchronized Set<TaskSplitMetaInfo> getNewSplitOnNode(String node, int blockNum,boolean locality) {

		Set<TaskSplitMetaInfo> taskSplitMetaInfos = getBlocksOnNode(node, blockNum);
		Set<TaskSplitMetaInfo> result= new LinkedHashSet<TaskSplitMetaInfo>();
		
		LOG.info("result size:"+result.size());
		
		for(TaskSplitMetaInfo taskSplitMetaInfo :taskSplitMetaInfos){
			
			LOCAL_DATA++;
			
			result.add(taskSplitMetaInfo);
		}
		
		if((taskSplitMetaInfos.size()< blockNum && locality) || blockNum<=taskSplitMetaInfos.size()){
			
			
			return result;
		}
		
		LOG.info("local data is:"+LOCAL_DATA);
		
		int remains = blockNum-taskSplitMetaInfos.size();
		List<String >nodeList = getFastestNNode(getSplitsDataNode());    //node sorted by speed                                   //add current node at tail
		nodeList.remove(node);
		int index = nodeList.size()-1;
		
		while(true){
			
			if(remains ==0 || index < 0){
					
			    break;
		     }
			
			Set<TaskSplitMetaInfo> temp = getBlocksOnNode(nodeList.get(index), remains);
			
            remains = remains - temp.size();
			
			index--;
			  
			for(TaskSplitMetaInfo split: temp){
				   result.add(split);
			}
		}
	
	  return result;	
	}


	@Override
	public synchronized Set<TaskSplitMetaInfo> getAutomatedSplitOnNode(String node,boolean locality) {
		
		//get the slowest node to be basement.
		String slowestNode = getSlowestNode();            //try to find the slowest node whose speed is not 0
		long  slowestSpeed=nodeToSpeed.get(slowestNode);
		
		long  BlockUnit = nodeToBlockUnit.get(node);    
	    long  BlockNumber;
       
        
        if(this.updateLastRoundTime(node)){             //if this is new round
        	
        	this.updateRound(node);
        	
        	if(this.nodeToRatio.get(node) < this.BLOCKS_UNIT_FAST_LIMIT){
        		
        		BlockUnit = BlockUnit * 2;
        		
        		LOG.info("updateblockunit"+node+"   x2 "+BlockUnit);
        	}else if(this.nodeToRatio.get(node) > this.BLOCKS_UNIT_FAST_LIMIT && 
        			 this.nodeToRatio.get(node) < this.BLOCKS_UNIT){
        		  		
        		BlockUnit = BlockUnit + 1;
        		
        		LOG.info("updateblockunit"+node+"   +1 "+BlockUnit);
        	}
        	
        	this.nodeToBlockUnit.put(node, BlockUnit);
        }
        
        if(nodeToSpeed.get(node)==0){ 	
        	BlockNumber = BlockUnit;
        }else if(node ==slowestNode){
        	BlockNumber = BlockUnit;	
        }else if(slowestSpeed==0){
        	BlockNumber = BlockUnit;
        }else{
        	
        	double speedRatio = (double)nodeToSpeed.get(node) / (double)slowestSpeed;
        	
        	if(speedRatio > MAX_SPEED_RATIO){
        		
        		speedRatio = MAX_SPEED_RATIO;
        	}
		
        	BlockNumber = (long)(Math.round(BlockUnit*speedRatio));
		    
        }
         
        nodeToBlockNum.put(node, BlockNumber);
        
        LOG.info("slowest node:"+slowestNode);
        LOG.info("slowest speed:"+slowestSpeed);
        
        
       
       LOG.info("BlockNum");
       LOG.info("blocknumunit:"+nodeToBlockUnit.get(node));
       LOG.info("blocknumspeed:"+nodeToSpeed.get(node));
       LOG.info("blocknumratio:"+nodeToRatio.get(node));
       LOG.info("blocknumblock:"+nodeToBlockNum.get(node));
       LOG.info("blocknumnode:"+node);
       LOG.info("/BlockNum");
        
       
		return getNewSplitOnNode(node,(int)BlockNumber,locality);
	}

}

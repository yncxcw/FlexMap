package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.MultiMapContextImpl;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.MapTask.NewDirectOutputCollector;
import org.apache.hadoop.mapred.MapTask.NewOutputCollector;
import org.apache.hadoop.mapred.MapTask.NewTrackingRecordReader;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** A Multi Map task. */
@InterfaceAudience.LimitedPrivate({"MultiMapReduce"})
@InterfaceStability.Unstable
public class MultiMapTask extends MapTask {
	
	private TaskSplitIndex[] splitMetaInfos;
	
	private static final Log LOG = LogFactory.getLog(MultiMapTask.class.getName());
	
	public MultiMapTask() {
	    super();
	}
	
	public MultiMapTask(String jobFile, TaskAttemptID taskId, 
            int partition, TaskSplitIndex[] splitIndex,
            int numSlotsRequired){
		super(jobFile,taskId,partition,splitIndex[0],numSlotsRequired);
		
		this.splitMetaInfos=splitIndex;
	}
	
	 @Override
	  public void write(DataOutput out) throws IOException {
		LOG.info("MultiMapTask serial start new");
	   // ((Task)this).write(out);
		super.write(out);
	    if (isMapOrReduce()) {	   
	    	  out.writeInt(splitMetaInfos.length);
	    	  LOG.info("serial write:splitlenth  "+splitMetaInfos.length);
	    	  
	      for(int i=0;i<splitMetaInfos.length;i++){
	    	  LOG.info("serial write split"+splitMetaInfos[i].toString());
	    	 
	    	  splitMetaInfos[i].write(out);
	      }
	     splitMetaInfos=null;
	    }
	  }
	  
	  @Override
	  public void readFields(DataInput in) throws IOException {
	    //((Task)this).readFields(in);	
		super.readFields(in);  		
	    if (isMapOrReduce()) {
	    	
	    	int splitLength=in.readInt();
	    	
	    	LOG.info("serial write:splitlenth"+splitLength);
	    	
	    	splitMetaInfos = new TaskSplitIndex[splitLength];
	    	
	    	for(int i=0;i<splitLength;i++){
	    		splitMetaInfos[i]=new TaskSplitIndex();
	    		splitMetaInfos[i].readFields(in);
	    		LOG.info("serial read"+splitMetaInfos[i].toString());
		    	 
		 }	
	    }
	  }
	  
	  
	@SuppressWarnings("unchecked")
	public <INKEY,INVALUE,OUTKEY,OUTVALUE>
	  void runNewMapper(final JobConf job,
	                    final TaskSplitIndex splitIndex,
	                    final TaskUmbilicalProtocol umbilical,
	                    TaskReporter reporter
	                    ) throws IOException, ClassNotFoundException,
	                             InterruptedException {
	    // make a task context so we can get the classes
	    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
	      new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job, 
	                                                                  getTaskID(),
	                                                                  reporter);
	    // make a mapper
	    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper =
	      (org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
	        ReflectionUtils.newInstance(taskContext.getMapperClass(), job);
	    // make the input format
	    org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE> inputFormat =
	      (org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE>)
	        ReflectionUtils.newInstance(taskContext.getInputFormatClass(), job);
	    // rebuild the input split
	    org.apache.hadoop.mapreduce.InputSplit[] splits= new org.apache.hadoop.mapreduce.InputSplit[splitMetaInfos.length];
	    LOG.info("Multi Map task NewMApper");
	    for(int i=0;i<splitMetaInfos.length;i++){
	    	
	      splits[i]=getSplitDetails(new Path(splitMetaInfos[i].getSplitLocation()),
	    			splitMetaInfos[i].getStartOffset());
	      LOG.info("Processing split: " + splits[i]);
	    	
	    }
	  
	    org.apache.hadoop.mapreduce.RecordReader<INKEY,INVALUE> input =
	      new NewMultiTrackingRecordReader<INKEY,INVALUE>
	        (splits, inputFormat, reporter, taskContext);
	    
	    LOG.info("initial RecoderReader");
	    
	    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
	    org.apache.hadoop.mapreduce.RecordWriter output = null;
	    
	    // get an output object
	    if (job.getNumReduceTasks() == 0) {
	      output = 
	        new NewDirectOutputCollector(taskContext, job, umbilical, reporter);
	    } else {
	      output = new NewOutputCollector(taskContext, job, umbilical, reporter);
	    }

	    
	    org.apache.hadoop.mapreduce.MapContext<INKEY, INVALUE, OUTKEY, OUTVALUE> 
	    mapContext = 
	      new MultiMapContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(job, getTaskID(), 
	          input, output, 
	          committer, 
	          reporter, splits);
	    
	    
	    
	    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context 
	        mapperContext = 
	          new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>().getMapContext(
	              mapContext);

	    try {
	      taskStatus.setMapBeginTime(System.currentTimeMillis());
	      input.initialize();
	      mapper.run(mapperContext);
	      mapPhase.complete();
	      setPhase(TaskStatus.Phase.SORT);
	      statusUpdate(umbilical);
	      input.close();
	      input = null;
	      output.close(mapperContext);
	      output = null;
	    } finally {
	      closeQuietly(input);
	      closeQuietly(output, mapperContext);
	    }
	  }
	  
	  
	  static class NewMultiTrackingRecordReader<K,V> 
	    extends org.apache.hadoop.mapreduce.RecordReader<K,V> {
	    private org.apache.hadoop.mapreduce.RecordReader<K,V> currReal;
	    private final org.apache.hadoop.mapreduce.Counter inputRecordCounter;
	    private final org.apache.hadoop.mapreduce.Counter fileInputByteCounter;
	    private final TaskReporter reporter;
	    private long splitsLength;
	    private  List<Statistics> fsStats;
	    org.apache.hadoop.mapreduce.InputFormat<K, V> inputFormat;
	    private int  currSplitIndex;
	    private long currProgress;
	    org.apache.hadoop.mapreduce.TaskAttemptContext context;
	    
	    private org.apache.hadoop.mapreduce.InputSplit splits[];
	    
	    NewMultiTrackingRecordReader(org.apache.hadoop.mapreduce.InputSplit splits[],
	        org.apache.hadoop.mapreduce.InputFormat<K, V> inputFormat,
	        TaskReporter reporter,
	        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
	        throws InterruptedException, IOException {
	      this.reporter = reporter;
	      this.inputRecordCounter = reporter
	          .getCounter(TaskCounter.MAP_INPUT_RECORDS);
	      this.fileInputByteCounter = reporter
	          .getCounter(FileInputFormatCounter.BYTES_READ);
	      this.context=taskContext;
	      this.splits=splits;
	      this.inputFormat=inputFormat;
	      this.splitsLength=0;	     
	       
	    }
	    
	    private long getSplitsLength() throws IOException, InterruptedException{
	    
	    if(splitsLength==0){
	   	
	    	long length=0;	       
		    for(org.apache.hadoop.mapreduce.InputSplit split:splits){
		    	   
		    	   length+=split.getLength();
		    }
		   splitsLength=length;
	    
	    }	
	    	
	     return splitsLength;
	     
	  }

	    @Override
	  public void close() throws IOException {
	      long bytesInPrev = getInputBytes(fsStats);
	      currReal.close();
	      long bytesInCurr = getInputBytes(fsStats);
	      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
	    }

	    @Override
	    public K getCurrentKey() throws IOException, InterruptedException {
	      return currReal.getCurrentKey();
	    }

	    @Override
	    public V getCurrentValue() throws IOException, InterruptedException {
	      return currReal.getCurrentValue();
	    }

	    @Override
	    public float getProgress() throws IOException, InterruptedException {
	      long subProgress = 0;
	      if(currReal!=null){
	    	  
	    	  subProgress = (long)(currReal.getProgress()*splits[currSplitIndex-1].getLength());
	      }
	      
	      //LOG.info("current progress"+currReal.getProgress()+"at split"+(this.currSplitIndex-1));
	      
	      return Math.min(1.0f, (currProgress+subProgress)/(float)(getSplitsLength()));
	    }
	   
	    public void initialize()throws IOException, InterruptedException{
	    	
	    	this.currSplitIndex = 0;
	    	
	    	List<Statistics> matchedStats = null;
	    	
	    	LOG.info("initialize input");
    		
	    	
	    	if (splits[0] instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
	    	
	    	LOG.info("path is"+((org.apache.hadoop.mapreduce.lib.input.FileSplit) splits[0])
            .getPath());
    	        matchedStats = getFsStatistics(((org.apache.hadoop.mapreduce.lib.input.FileSplit) splits[0])
    	            .getPath(), context.getConfiguration());
    	    }
    	      fsStats = matchedStats;
    	     
    	    
    	      
    	    long bytesInPrev = getInputBytes(fsStats);
    	    this.currReal = inputFormat.createRecordReader(splits[0], context);
    	    long bytesInCurr = getInputBytes(fsStats);
    	    fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
	    	
    	    
	    	bytesInPrev = getInputBytes(fsStats);
		    currReal.initialize(splits[0], context);
		    bytesInCurr = getInputBytes(fsStats);
		    fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
		    
		    LOG.info("initialize input3");
		    
		    this.currSplitIndex++;
	    		    	
	    }
	    
	    public boolean initNextSplit() throws IOException, InterruptedException{
	    	
	    	LOG.info("init next split at "+currSplitIndex);
	    	
            if(currSplitIndex==splits.length){
	    		
	    		LOG.info("finish doing this task");
	    		
	    		return false;
	    	}
	    	//to reset currReal
	    	if(currReal != null){ 
	    		 		
	    		close();
	    		
	    		currReal=null;
	    		
	    		if(currSplitIndex > 0){
	    			
	    			currProgress+=splits[currSplitIndex-1].getLength(); //done till now
	    		}
	    		
	    	}
	    	
	   
	    	//to reset fsStats
	    	if(fsStats!=null){
	    		
	    		List<Statistics> matchedStats = null;
	    		if (splits[currSplitIndex] instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
	    	        matchedStats = getFsStatistics(((org.apache.hadoop.mapreduce.lib.input.FileSplit) splits[currSplitIndex])
	    	            .getPath(), context.getConfiguration());
	    	      }
	    	      fsStats = matchedStats;
	    	      
	    	    long bytesInPrev = getInputBytes(fsStats);
	    	    this.currReal = inputFormat.createRecordReader(splits[currSplitIndex], context);
	    	    long bytesInCurr = getInputBytes(fsStats);
	    	    fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
	    	      
	    	}
	    	
	    	//initialize reacorder reader
	    	try{
	    	  LOG.info("initialize new recorder reader"+currSplitIndex);	
	    	  long bytesInPrev = getInputBytes(fsStats);
	    	  currReal.initialize(splits[currSplitIndex], context);	
	    	  long bytesInCurr = getInputBytes(fsStats);
	    	  fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
	    	
	    	}catch(Exception e){
	    		
	    	   throw new RuntimeException(e);	
	    	}
	    	
	    	currSplitIndex++;
	    	
	    	return true;
	    	
	    }

	    @Override
	    public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
	                           org.apache.hadoop.mapreduce.TaskAttemptContext context
	                           ) throws IOException, InterruptedException {
	     
	    }

	    @Override
	    public boolean nextKeyValue() throws IOException, InterruptedException {
	      long bytesInPrev = getInputBytes(fsStats);
	      while(currReal ==null || !currReal.nextKeyValue()) {	 //potential nullpoiner exception for currReal       
	    	  	      	    	  
	    	 if(!initNextSplit()){	   
	    		 LOG.info("finish this task");
	    		 return false;
	    	 } 	  
	      }	      
	      inputRecordCounter.increment(1);	        
    	  long bytesInCurr = getInputBytes(fsStats);
	      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
	      reporter.setProgress(getProgress());
	      return true;
	    }

	    private long getInputBytes(List<Statistics> stats) {
	      if (stats == null) return 0;
	      long bytesRead = 0;
	      for (Statistics stat: stats) {
	        bytesRead = bytesRead + stat.getBytesRead();
	      }
	      return bytesRead;
	    }
	  }
	 
}

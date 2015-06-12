package org.apache.hadoop.mapreduce.task;

import org.apache.hadoop.mapreduce.MapContext;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * The context that is given to the {@link Mapper}.
 * @param <KEYIN> the key input type to the Mapper
 * @param <VALUEIN> the value input type to the Mapper
 * @param <KEYOUT> the key output type from the Mapper
 * @param <VALUEOUT> the value output type from the Mapper
 */

public class MultiMapContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT>  
  extends TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
  implements MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
	private RecordReader<KEYIN,VALUEIN> reader;
	private InputSplit[] split;

	  public MultiMapContextImpl(Configuration conf, TaskAttemptID taskid,
	                        RecordReader<KEYIN,VALUEIN> reader,
	                        RecordWriter<KEYOUT,VALUEOUT> writer,
	                        OutputCommitter committer,
	                        StatusReporter reporter,
	                        InputSplit[] split) {
	    super(conf, taskid, writer, committer, reporter);
	    this.reader = reader;
	    this.split = split;
	  }

	  
	  /**
	   * Get the first input split for this map.
	   */
	  public InputSplit getInputSplit() {
	    return split[0];
	  }
	  
	  public InputSplit getInputSplit(int idx){
		return split[idx];  
	  }

	  @Override
	  public KEYIN getCurrentKey() throws IOException, InterruptedException {
	    return reader.getCurrentKey();
	  }

	  @Override
	  public VALUEIN getCurrentValue() throws IOException, InterruptedException {
	    return reader.getCurrentValue();
	  }

	  @Override
	  public boolean nextKeyValue() throws IOException, InterruptedException {
	    return reader.nextKeyValue();
	  }
	

}

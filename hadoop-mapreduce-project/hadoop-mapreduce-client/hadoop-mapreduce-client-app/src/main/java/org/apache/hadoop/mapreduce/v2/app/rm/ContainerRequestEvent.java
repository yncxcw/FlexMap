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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;


public class ContainerRequestEvent extends ContainerAllocatorEvent {
  
  private final Resource capability;
  private final String[] hosts;
  private final String[] racks;
  private final boolean  nodeRelaxLocality;
  private final boolean  rackRelaxLocality;
  private final boolean  anyRelaxLocality;
  private boolean earlierAttemptFailed = false;

  public ContainerRequestEvent(TaskAttemptId attemptID, 
      Resource capability,boolean nodeRelaxLocality,boolean rackRelaxLocality,boolean anyRelaxLocality,
      String[] hosts, String[] racks) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_REQ);
    this.capability = capability;
    this.hosts = hosts;
    this.racks = racks;
    this.nodeRelaxLocality = nodeRelaxLocality;
    this.rackRelaxLocality = rackRelaxLocality;
    this.anyRelaxLocality  = anyRelaxLocality;
  }
  
  public ContainerRequestEvent(TaskAttemptId attemptID, Resource capability){ 
    super(attemptID, ContainerAllocator.EventType.CONTAINER_REQ);
    this.capability = capability;
    this.hosts = new String[0];
    this.racks = new String[0];
    this.earlierAttemptFailed = true;
    this.nodeRelaxLocality = true;
    this.rackRelaxLocality = true;
    this.anyRelaxLocality  = true;
  }
  
  public static ContainerRequestEvent createContainerRequestEventForFailedContainer(
      TaskAttemptId attemptID, 
      Resource capability) {
    //ContainerRequest for failed events does not consider rack / node locality?
    return new ContainerRequestEvent(attemptID, capability);
  }

  
  public Resource getCapability() {
    return capability;
  }

  public String[] getHosts() {
    return hosts;
  }
  
  public String[] getRacks() {
    return racks;
  }
  
  public boolean getEarlierAttemptFailed() {
    return earlierAttemptFailed;
  }
  
  public boolean isNodeRelaxLocality() {
		return nodeRelaxLocality;
   }

  public boolean isRackRelaxLocality() {
		return rackRelaxLocality;
   }

	
  public boolean isAnyRelaxLocality() {
		
	   return anyRelaxLocality;
	
  }

  
  
}
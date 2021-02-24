/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cloud.autoscaling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.AsyncCollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to verify the functionality of {@link RebalanceReplicasPlanAction} trigger action.
 */
public class RebalanceReplicasPlanActionTest extends SolrCloudTestCase {
  private static String triggerName = "rebalance_trigger";
  private static String collectionName = "testRebalance";
  private static String node1;
  private static String node2;
  private static CountDownLatch triggerFiredLatch;
  private static CountDownLatch executorRanLatch;

  @Before
  public void beforeTest() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    TestInjection.reset();
  }
  
  @After 
  public void afterTest() throws Exception {
    shutdownCluster();
  }
  
  /**
   * Test that replicas are more distributed after the trigger runs than they were before.
   */
  @Test
  public void testRebalance() throws Exception {
    ClusterState initClusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
    List<String> liveNodes = new ArrayList<String>(2);
    liveNodes.addAll(initClusterState.getLiveNodes());
    
    // Create collection with 10 replicas on a single node
    node1 = liveNodes.get(0);
    node2 = liveNodes.get(1);
    CollectionAdminRequest.Create createCollection = 
        CollectionAdminRequest.createCollection(collectionName, 10, 1)
        .setCreateNodeSet(node1)
        .setMaxShardsPerNode(-1);
    cluster.getSolrClient().request(createCollection);
    
    // Wait for collection to be created
    cluster.waitForActiveCollection(collectionName, 10, 10);
    
    RebalanceReplicasPlanAction rebalancer = new TestRebalanceReplicasPlanAction();
    rebalancer.configure(null, cluster.getOpenOverseer().getSolrCloudManager(), new HashMap<String,Object>(0));
    
    ExecutePlanAction executor = new TestExecutePlanAction();
    executor.configure(null, cluster.getOpenOverseer().getSolrCloudManager(), new HashMap<String,Object>(0));
    try {
      triggerFiredLatch = new CountDownLatch(1);
      executorRanLatch = new CountDownLatch(1);
      
      // Verify that there are 10 replicas on 1 node
      ClusterState clusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
      DocCollection collectionBeforeRebalance = clusterState.getCollection(collectionName);
      assertTrue(collectionBeforeRebalance.getReplicas(node1).size() == 10);
      
      // Create required variables
      ScheduledTrigger trigger = new ScheduledTrigger(triggerName);
      ActionContext context = new ActionContext(cluster.getOpenOverseer().getSolrCloudManager(), 
          trigger, new HashMap<String, Object>());
      TriggerEvent scheduledEvent = 
          new ScheduledTrigger.ScheduledEvent(TriggerEventType.SCHEDULED, "rebalancer", 100, "", 100);
      
      // Run rebalancer to compute moves
      rebalancer.process(scheduledEvent, context);
      triggerFiredLatch.await(5, TimeUnit.SECONDS);
      
      // Execute moves
      executor.process(scheduledEvent, context);
      executorRanLatch.await(5, TimeUnit.SECONDS);
      
      // See which nodes have replicas
      ClusterState finalClusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
      DocCollection collectionAfterRebalance = finalClusterState.getCollection(collectionName);
      List<Replica> node1Replicas = collectionAfterRebalance.getReplicas(node1);
      List<Replica> node2Replicas = collectionAfterRebalance.getReplicas(node2);
      
      // Verify that replica count for each node is within threshold
      assertTrue(node1Replicas.size() < 7);
      assertTrue(node2Replicas.size() > 3);
    } finally {
      rebalancer.close();
      executor.close();
    }
  }
  
  /**
   * Test that configuring a scheduled trigger with RebalanceReplicasPlanAction as the TriggerAction
   * succeeds.
   */
  @SuppressWarnings("rawtypes")
  @Test
  public void testConfigure() throws Exception {
    // Configure rebalancer trigger
    String setConfigPayload = 
        "{ 'set-trigger': { 'name' : '" + triggerName + "', 'event' : 'scheduled'," +
        "'startTime' : 'NOW', 'every' : '+2MINUTES', 'enabled' : true, 'actions' : " +
        "[ { 'name' : 'rebalance', 'class': 'solr.RebalanceReplicasPlanAction', 'threshold': '20' }, " + 
        "{ 'name' : 'execute_plan', 'class': 'solr.ExecutePlanAction' } ] }}";
    
    SolrRequest setReq = AutoScalingRequest.create(SolrRequest.METHOD.POST, setConfigPayload);
    cluster.getSolrClient().request(setReq);
    CloudTestUtils.waitForTriggerToBeScheduled(cluster.getOpenOverseer().getSolrCloudManager(), triggerName);
    
    String deleteConfigPayload = 
        "{ 'remove-trigger': { 'name' : '" + triggerName + "'}}";
    SolrRequest deleteReq = AutoScalingRequest.create(SolrRequest.METHOD.POST, deleteConfigPayload);
    cluster.getSolrClient().request(deleteReq);
  }
  
  /**
   * Test-only child of RebalanceReplicasPlanAction, to allow for countdown latch.
   */
  public static class TestRebalanceReplicasPlanAction extends RebalanceReplicasPlanAction {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      super.process(event, context);

      @SuppressWarnings("unchecked")
      List<AsyncCollectionAdminRequest> ops = ((ArrayList<AsyncCollectionAdminRequest>)context.getProperties().get("operations"));
      assertTrue(ops.size() > 0);
      triggerFiredLatch.countDown();
    }
  }
  
  /**
   * Test-only child of ExecutePlanAction, to allow for countdown latch.
   */
  public static class TestExecutePlanAction extends ExecutePlanAction {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      super.process(event, context);
      executorRanLatch.countDown();
    }
  }
}

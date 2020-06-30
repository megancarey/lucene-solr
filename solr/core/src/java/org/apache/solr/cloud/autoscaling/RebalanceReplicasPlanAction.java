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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.AsyncCollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.MoveReplica;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This trigger action evaluates the state of the cluster and determines a set of replica moves
 * that will improve the distribution of replicas throughout the cluster.
 * 
 * The {@link MoveReplica} requests computed are put into the {@link ActionContext}'s properties
 * with the key name "operations", to be executed later by {@link ExecutePlanAction}.
 */
public class RebalanceReplicasPlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /*
   * Functions for computing map values
   */
  public static final Function<String, Map<CollectionShardKey,List<Replica>>> LIST_MAP_FUNCTION =
      nodeName -> new HashMap<CollectionShardKey,List<Replica>>();
  public static final Function<CollectionShardKey, List<Replica>> REPLICA_LIST_FUNCTION =
      collShardName -> new ArrayList<Replica>();
  public static final Function<String, Integer> ZERO_FUNCTION = nodeName -> 0;
  
  /** Configuration property for setting percentage difference in replica count across nodes */
  public static final String THRESHOLD_PROP = "threshold";
  
  /** Default threshold for percent difference in replica count across nodes */
  public static final int DEFAULT_THRESHOLD = 10;

  private int diffThreshold;
  
  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    String thresholdStr = String.valueOf(properties.getOrDefault(THRESHOLD_PROP, String.valueOf(DEFAULT_THRESHOLD)));
    try {
      diffThreshold = Integer.parseInt(thresholdStr);
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), THRESHOLD_PROP, "invalid value '" + thresholdStr + "': " + e.toString());
    }
    if (diffThreshold < 0 || diffThreshold > 100) {
      throw new TriggerValidationException(getName(), THRESHOLD_PROP, "invalid value '" + thresholdStr + "', should be > 0 and < 100. ");
    }
  }

  @Override
  public void process(TriggerEvent event, ActionContext context) throws Exception {
    if (!context.getProperties().containsKey("operations")) {
      context.getProperties().put("operations", new ArrayList<AsyncCollectionAdminRequest>());
    }
    
    // Get state and list of ops to modify
    @SuppressWarnings({"unchecked"})
    List<AsyncCollectionAdminRequest> operations = (List<AsyncCollectionAdminRequest>) context.getProperties().get("operations");
    SolrCloudManager cloudManager = context.getCloudManager();
    ClusterState state = cloudManager.getClusterStateProvider().getClusterState();

    // Init variables
    int sumReplicas = 0;
    List<NodeReplicaCount> nodeReplicaCounts = new ArrayList<NodeReplicaCount>(state.getLiveNodes().size());
    HashMap<String,Map<CollectionShardKey,List<Replica>>> replicasByShardByNodeMap =
        new HashMap<String,Map<CollectionShardKey,List<Replica>>>();

    // For each collection, add replicas to inverted state map of node -> replicas
    Iterator<String> collIter = state.getCollectionsMap().keySet().iterator();
    while (collIter.hasNext()) {
      DocCollection coll = state.getCollection(collIter.next());
      List<Replica> replicas = coll.getReplicas();
      
      replicas.stream().forEach(replica -> 
        replicasByShardByNodeMap
          .computeIfAbsent(replica.getNodeName(), LIST_MAP_FUNCTION)
            .computeIfAbsent(new CollectionShardKey(replica.collection, replica.slice), REPLICA_LIST_FUNCTION)
            .add(replica));
            
      // Increment count of all replicas
      sumReplicas += replicas.size();
    }
    
    // Create list of NodeReplicaCount objects
    for (String node : state.getLiveNodes()) {
      int numReplicas = 0;
      if (replicasByShardByNodeMap.containsKey(node)) {
        numReplicas = replicasByShardByNodeMap.get(node).values()
            .stream()
            .map(list -> list.size())
            .reduce(0, Integer::sum);
      }
      nodeReplicaCounts.add(new NodeReplicaCount(node, numReplicas));
    }
    
    // Determine which nodes have above average concentration of replicas, and which have below average concentration
    int avgReplicasPerNode = sumReplicas / nodeReplicaCounts.size();
    Collections.sort(nodeReplicaCounts);
    int numNodes = nodeReplicaCounts.size();
    
    for (int i = 0; i < numNodes / 2; i++) {
      // Get least loaded node, toNode, and most loaded node, fromNode
      NodeReplicaCount toNode = nodeReplicaCounts.get(i);
      NodeReplicaCount fromNode = nodeReplicaCounts.get(numNodes - i - 1);
      
      // Get the replica maps for each node
      Map<CollectionShardKey,List<Replica>> toNodeReplicaMap = replicasByShardByNodeMap.get(toNode.nodeName);
      Map<CollectionShardKey,List<Replica>> fromNodeReplicaMap = replicasByShardByNodeMap.get(fromNode.nodeName);
      
      // Calculate the set of collShards that are distinct on fromNode
      HashSet<CollectionShardKey> outersection = new HashSet<CollectionShardKey>(fromNodeReplicaMap.keySet());
      if (toNodeReplicaMap != null) {
        outersection.removeAll(toNodeReplicaMap.keySet());
      }
      
      // If this underloaded node already has a copy of each replica on the overloaded node, skip
      if (outersection.size() == 0) {
        continue;
      }
      
      // For each collShard on the overloaded node, move a replica to the underloaded node, until the
      // nodes have balanced out.
      Iterator<CollectionShardKey> keyIter = outersection.iterator();
      while (keyIter.hasNext() && 
          toNode.replicaCount < avgReplicasPerNode && fromNode.replicaCount > avgReplicasPerNode) {
        CollectionShardKey key = keyIter.next();
        List<Replica> replicasToMove = fromNodeReplicaMap.remove(key);
        
        if (replicasToMove != null && replicasToMove.size() > 0) {
          Collections.shuffle(replicasToMove);
          Replica replica = replicasToMove.get(0);
          operations.add(
              CollectionAdminRequest.moveReplica(replica.collection, replica.getName(), toNode.nodeName));
          toNode.replicaCount += 1;
          fromNode.replicaCount -= 1;
        }
      }
    }
  }
  
  private class CollectionShardKey {
    private String collectionName;
    private String shardName;
    
    public CollectionShardKey(String collectionName, String shardName) {
      this.collectionName = collectionName;
      this.shardName = shardName;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CollectionShardKey) {
        CollectionShardKey key = (CollectionShardKey) obj;
        return key.collectionName.equals(this.collectionName) && key.shardName.equals(this.shardName);
      }
      
      return false;
    }
    
    @Override
    public int hashCode() {
      return (collectionName + shardName).hashCode();
    }
  }
  
  private class NodeReplicaCount implements Comparable<NodeReplicaCount> {
    private String nodeName;
    private int replicaCount;
    
    public NodeReplicaCount(String nodeName, int replicaCount) {
      this.nodeName = nodeName;
      this.replicaCount = replicaCount;
    }
    
    @Override
    public int compareTo(NodeReplicaCount other) {
      return this.replicaCount - other.replicaCount;
    }
  }
}

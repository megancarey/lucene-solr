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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.AsyncCollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.MoveReplica;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.SolrResourceLoader;

/**
 * This trigger action evaluates the state of the cluster and determines a set of replica moves
 * that will improve the distribution of replicas throughout the cluster.
 * 
 * Sample configuration request body:
 * '{ "set-trigger":
 *    { "name": "rebalance_trigger", "event": "scheduled", "startTime": "NOW",
 *      "every": "+1MINUTES", "enabled": true, "actions": 
 *      [ { "name": "rebalance_plan", "class": "solr.RebalanceReplicasPlanAction", "threshold": "20" }, 
 *        { "name": "execute_plan", "class": "solr.ExecutePlanAction" } ]
 *    }
 *  }'
 * 
 * The {@link MoveReplica} requests computed are put into the {@link ActionContext}'s properties
 * with the key name "operations", to be executed later by {@link ExecutePlanAction}.
 */
public class RebalanceReplicasPlanAction extends TriggerActionBase {
  /*
   * Functions for computing map values
   */
  public static final Function<String, Map<CollectionShardKey,List<Replica>>> LIST_MAP_FUNCTION =
      nodeName -> new HashMap<CollectionShardKey,List<Replica>>();
  public static final Function<CollectionShardKey, List<Replica>> REPLICA_LIST_FUNCTION =
      collShardName -> new ArrayList<Replica>();
  
  /** Configuration property for setting percentage difference in replica count across nodes */
  public static final String THRESHOLD_PROP = "threshold";
  
  /** Default threshold for percent difference in replica count across nodes */
  public static final int DEFAULT_THRESHOLD = 10;

  private int diffThreshold;
  
  public RebalanceReplicasPlanAction() {
    super();
    TriggerUtils.validProperties(validProperties, THRESHOLD_PROP);
  }
  
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
    
    @SuppressWarnings({"unchecked"})
    List<AsyncCollectionAdminRequest> operations = (List<AsyncCollectionAdminRequest>) context.getProperties().get("operations");
    SolrCloudManager cloudManager = context.getCloudManager();
    ClusterState state = cloudManager.getClusterStateProvider().getClusterState();
    Set<String> liveNodes = state.getLiveNodes();

    int sumReplicas = 0;
    List<NodeReplicaCount> nodeReplicaCounts = new ArrayList<NodeReplicaCount>(liveNodes.size());
    HashMap<String,Map<CollectionShardKey,List<Replica>>> replicasByShardByNodeMap =
        new HashMap<String,Map<CollectionShardKey,List<Replica>>>();

    // For each collection, add replicas to inverted state map of node -> replicas
    Iterator<String> collIter = state.getCollectionsMap().keySet().iterator();
    while (collIter.hasNext()) {
      DocCollection coll = state.getCollection(collIter.next());
      Set<String> activeSlices = coll.getActiveSlicesMap().keySet();
      
      // Filter out inactive replicas and replicas for inactive shards
      List<Replica> activeReplicas = coll.getReplicas().stream()
          .filter(replica -> replica.isActive(liveNodes) && activeSlices.contains(replica.getSlice()))
          .collect(Collectors.toList());
      
      activeReplicas.stream()
        .forEach(replica -> 
          replicasByShardByNodeMap
            .computeIfAbsent(replica.getNodeName(), LIST_MAP_FUNCTION)
              .computeIfAbsent(new CollectionShardKey(replica.collection, replica.slice), REPLICA_LIST_FUNCTION)
              .add(replica)
        );
            
      sumReplicas += activeReplicas.size();
    }
    
    // Create list of NodeReplicaCount objects
    for (String node : liveNodes) {
      int numReplicas = 0;
      if (replicasByShardByNodeMap.containsKey(node)) {
        numReplicas = replicasByShardByNodeMap.get(node).values()
            .stream()
            .map(list -> list.size())
            .reduce(0, Integer::sum);
      }
      nodeReplicaCounts.add(new NodeReplicaCount(node, numReplicas));
    }
    
    int avgReplicasPerNode = sumReplicas / nodeReplicaCounts.size();
    Collections.sort(nodeReplicaCounts);
    int numNodes = liveNodes.size();
    
    for (int i = 0; i < numNodes / 2; i++) {
      // Get least loaded node, toNode, and most loaded node, fromNode
      NodeReplicaCount toNode = nodeReplicaCounts.get(i);
      NodeReplicaCount fromNode = nodeReplicaCounts.get(numNodes - i - 1);
      
      Map<CollectionShardKey,List<Replica>> toNodeReplicaMap = replicasByShardByNodeMap.get(toNode.nodeName);
      Map<CollectionShardKey,List<Replica>> fromNodeReplicaMap = replicasByShardByNodeMap.get(fromNode.nodeName);
      
      // Calculate the set of collShards that are distinct on fromNode
      HashSet<CollectionShardKey> outersection = new HashSet<CollectionShardKey>(fromNodeReplicaMap.keySet());
      if (toNodeReplicaMap != null) {
        outersection.removeAll(toNodeReplicaMap.keySet());
      }
      
      // Consider nodes balanced when core counts are within <diffThreshold>% of average
      Iterator<CollectionShardKey> keyIter = outersection.iterator();
      float overMultiplier = 1 + (1f / diffThreshold);
      float underMultiplier = 1 - (1f / diffThreshold);
      
      // For each collShard on the overloaded node, move a replica to the underloaded node, until the
      // nodes have balanced out
      while (keyIter.hasNext() && 
          toNode.replicaCount < (avgReplicasPerNode * underMultiplier) &&
          fromNode.replicaCount > (avgReplicasPerNode * overMultiplier)) {
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
  
  /**
   * A composite key with both collection name and shard name, used as a key in the map of replicas
   * for each node.
   */
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
  
  /**
   * An object containing both node name and the count of replicas/cores on that node. Used to
   * determine candidate nodes for replica moves to improve distribution of cores in the cluster.
   */
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

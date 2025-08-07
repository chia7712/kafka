/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class MagicAdminTest {

    @ClusterTest(types = {Type.KRAFT}, brokers = 3, controllers = 3)
    void testControllerFromBroker(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            var controller0 = admin.describeCluster().controller().get();
            var controller1 = admin.describeCluster().controller().get();
            assertEquals(controller0, controller1); // ???
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3, controllers = 3)
    void testControllerFromQuorum(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin(Map.of(), true)) {
            var controller0 = admin.describeCluster().controller().get();
            var controller1 = admin.describeCluster().controller().get();
            assertEquals(controller0, controller1); // ???
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3)
    void testNodesAndClusterId(ClusterInstance clusterInstance) throws Exception {
        try (var brokerAdmin = clusterInstance.admin();
             var quorumAdmin = clusterInstance.admin(Map.of(), true)) {

            // 1) check the cluster id
            var clusterIdFromBroker = brokerAdmin.describeCluster().clusterId().get();
            var clusterIdFromQuorum = quorumAdmin.describeCluster().clusterId().get();
            assertEquals(clusterIdFromBroker, clusterIdFromQuorum); // ???

            // 1) check the nodes
            var nodesFromBroker = brokerAdmin.describeCluster().nodes().get();
            var nodesFromQuorum = quorumAdmin.describeCluster().nodes().get();
            assertEquals(nodesFromBroker, nodesFromQuorum); // ???
        }
    }


    @ClusterTest(types = {Type.KRAFT}, brokers = 3)
    void testExcludeLeaderByAssignment(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            // 1) collect all nodes
            var nodes = admin.describeCluster().nodes().get().stream().map(Node::id).toList();

            // 2) create topics
            admin.createTopics(List.of(new NewTopic("chia", 1, (short) 1))).all().get();

            // 3) sleep for metadata
            TimeUnit.SECONDS.sleep(2);

            // 4) check leader
            var leader = admin.describeTopics(List.of("chia")).allTopicNames().get().get("chia").partitions().get(0).leader();

            // 5) exclude leader
            var others = nodes.stream().filter(id -> leader.id() != id).toList();
            admin.alterPartitionReassignments(Map.of(new TopicPartition("chia", 0), Optional.of(new NewPartitionReassignment(others)))).all().get();
            TimeUnit.SECONDS.sleep(2);

            // 6) check leader
            var newLeader = admin.describeTopics(List.of("chia")).allTopicNames().get().get("chia").partitions().get(0).leader();

            assertNotEquals(leader, newLeader);  // ???
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3)
    void testChangeLeaderByAssignment(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            // 1) collect all nodes
            var nodes = admin.describeCluster().nodes().get().stream().map(Node::id).toList();

            // 2) create topics
            admin.createTopics(List.of(new NewTopic("chia", 1, (short) 1))).all().get();

            // 3) sleep for metadata
            TimeUnit.SECONDS.sleep(2);

            // 4) check leader
            var oldLeader = admin.describeTopics(List.of("chia")).allTopicNames().get().get("chia").partitions().get(0).leader();

            // 5) change leader
            var newReplicas = new ArrayList<>(nodes.stream().filter(id -> oldLeader.id() != id).toList());
            newReplicas.add(oldLeader.id());
            admin.alterPartitionReassignments(Map.of(new TopicPartition("chia", 0), Optional.of(new NewPartitionReassignment(newReplicas)))).all().get();
            TimeUnit.SECONDS.sleep(2);

            // 6) check leader
            var newLeader = admin.describeTopics(List.of("chia")).allTopicNames().get().get("chia").partitions().get(0).leader();

            assertNotEquals(oldLeader, newLeader); // ???

            // 7) run election
            admin.electLeaders(ElectionType.PREFERRED, Set.of(new TopicPartition("chia", 0))).all().get();
            TimeUnit.SECONDS.sleep(2);

            newLeader = admin.describeTopics(List.of("chia")).allTopicNames().get().get("chia").partitions().get(0).leader();
            assertNotEquals(oldLeader, newLeader); // ???
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3, serverProperties = {
        @ClusterConfigProperty(key = "leader.imbalance.check.interval.seconds", value = "5")
    })
    void testChangeLeaderByAssignmentWithAuto(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            // 1) collect all nodes
            var nodes = admin.describeCluster().nodes().get().stream().map(Node::id).toList();

            // 2) create topics
            admin.createTopics(List.of(new NewTopic("chia", 1, (short) 1))).all().get();

            // 3) sleep for metadata
            TimeUnit.SECONDS.sleep(2);

            // 4) check leader
            var oldLeader = admin.describeTopics(List.of("chia")).allTopicNames().get().get("chia").partitions().get(0).leader();

            // 5) change leader
            var newReplicas = new ArrayList<>(nodes.stream().filter(id -> oldLeader.id() != id).toList());
            newReplicas.add(oldLeader.id());
            admin.alterPartitionReassignments(Map.of(new TopicPartition("chia", 0), Optional.of(new NewPartitionReassignment(newReplicas)))).all().get();

            // wait for auto-leader-balance
            TimeUnit.SECONDS.sleep(8);

            var newLeader = admin.describeTopics(List.of("chia")).allTopicNames().get().get("chia").partitions().get(0).leader();
            assertNotEquals(oldLeader, newLeader);
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3, serverProperties = {
        @ClusterConfigProperty(key = "delete.topic.enable", value = "false")
    })
    void testDeleteTopic(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            admin.createTopics(List.of(new NewTopic("chia", 1, (short) 1))).all().get();
            TimeUnit.SECONDS.sleep(2);

            admin.deleteTopics(List.of("chia")).all().get();
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3)
    void testReduceReplicas(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            // 1) collect all nodes
            var nodes = admin.describeCluster().nodes().get().stream().map(Node::id).toList();

            admin.createTopics(List.of(new NewTopic("chia", 1, (short) 3))).all().get();
            admin.alterPartitionReassignments(Map.of(new TopicPartition("chia", 0), Optional.of(new NewPartitionReassignment(List.of(nodes.get(0))))),
                    new AlterPartitionReassignmentsOptions().allowReplicationFactorChange(false) // ???
            ).all().get();
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3, serverProperties = {
        @ClusterConfigProperty(key = "message.max.bytes", value = "1048588")
    })
    void testClusterLevelConfig(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            // 1) change the cluster-level value
            admin.incrementalAlterConfigs(Map.of(
                    new ConfigResource(ConfigResource.Type.BROKER, ""), // cluster-level resource
                        List.of(new AlterConfigOp(new ConfigEntry("message.max.bytes", "10485880"), AlterConfigOp.OpType.SET))))
                    .all()
                    .get();
            TimeUnit.SECONDS.sleep(2);

            // 2) check the cluster-level value
            var value = admin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.BROKER, ""))).all().get().values().stream().flatMap(s -> s.entries().stream())
                    .filter(e -> e.name().equals("message.max.bytes"))
                    .findAny()
                    .get()
                    .value();
            assertEquals("10485880", value);
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3, serverProperties = {
        @ClusterConfigProperty(key = "message.max.bytes", value = "1048588")
    })
    void testClusterLevelConfigOnQuorum(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin(Map.of(), true)) {
            // 1) change the cluster-level value by quorum
            admin.incrementalAlterConfigs(Map.of(
                            new ConfigResource(ConfigResource.Type.BROKER, ""), // cluster-level resource
                            List.of(new AlterConfigOp(new ConfigEntry("message.max.bytes", "10485880"), AlterConfigOp.OpType.SET))))
                    .all()
                    .get();
            TimeUnit.SECONDS.sleep(2);
        }

        try (var admin = clusterInstance.admin()) {
            // 2) check the cluster-level value of broker
            var value = admin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.BROKER, ""))).all().get().values().stream().flatMap(s -> s.entries().stream())
                    .filter(e -> e.name().equals("message.max.bytes"))
                    .findAny()
                    .get()
                    .value();
            assertEquals("10485880", value);
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3)
    void testClusterLevelConfigForLogger(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            admin.incrementalAlterConfigs(Map.of(
                            new ConfigResource(ConfigResource.Type.BROKER_LOGGER, ""), // cluster-level logger?
                            List.of(new AlterConfigOp(new ConfigEntry("root", "info"), AlterConfigOp.OpType.SET))))
                    .all()
                    .get();
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3)
    void testBrokerLevelConfigForLogger(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            admin.incrementalAlterConfigs(Map.of(
                            new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0"), // broker-level logger?
                            List.of(new AlterConfigOp(new ConfigEntry("root", "INFO"), AlterConfigOp.OpType.SET))))
                    .all()
                    .get();
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 3)
    void testGroupMetadata(ClusterInstance clusterInstance) throws Exception {
        try (var admin = clusterInstance.admin()) {
            // 1) create a topic with two partitions
            admin.createTopics(List.of(new NewTopic("chia", 2, (short) 1))).all().get();
            TimeUnit.SECONDS.sleep(2);

            // 2) send record to first partition
            try (var producer = clusterInstance.producer()) {
                producer.send(new ProducerRecord<>("chia", 0, "foo".getBytes(), "foo".getBytes())).get();
            }

            // 3) poll data and create committed offsets
            try (var consumer = clusterInstance.consumer(Map.of("group.id", "ikea"))) {
                consumer.subscribe(List.of("chia"));
                var count = consumer.poll(Duration.ofSeconds(1)).count();
                count += consumer.poll(Duration.ofSeconds(1)).count();
                count += consumer.poll(Duration.ofSeconds(1)).count();
                count += consumer.poll(Duration.ofSeconds(1)).count();
                assertEquals(1, count);
            }

            // how many committed offsets?

            var results = admin.listConsumerGroupOffsets("ikea").partitionsToOffsetAndMetadata("ikea").get();

            // 4) check first partition
            assertTrue(results.containsKey(new TopicPartition("chia", 0)));
            assertNotNull(results.get(new TopicPartition("chia", 0)));

            // 5) check second partition ... ?
            assertTrue(results.containsKey(new TopicPartition("chia", 1)));
            assertNotNull(results.get(new TopicPartition("chia", 1)));

            // 6) delete the committed offset explicitly
            admin.deleteConsumerGroupOffsets("ikea", Set.of(new TopicPartition("chia", 1))).all().get();
            TimeUnit.SECONDS.sleep(2);

            results = admin.listConsumerGroupOffsets("ikea").partitionsToOffsetAndMetadata("ikea").get();

            // 7) check second partition again ... ?
            assertTrue(results.containsKey(new TopicPartition("chia", 1)));
            assertNotNull(results.get(new TopicPartition("chia", 1)));
        }
    }

}
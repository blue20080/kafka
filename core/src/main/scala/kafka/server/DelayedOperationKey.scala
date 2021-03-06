/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.common.TopicAndPartition

/**
 * Keys used for delayed operation metrics recording
 */
trait DelayedOperationKey {
  def keyLabel: String
}

object DelayedOperationKey {
  val globalLabel = "All"
}

/* used by delayed-produce and delayed-fetch operations */
case class TopicPartitionOperationKey(topic: String, partition: Int) extends DelayedOperationKey {

  def this(topicAndPartition: TopicAndPartition) = this(topicAndPartition.topic, topicAndPartition.partition)

  override def keyLabel = "%s-%d".format(topic, partition)
}

/* used by delayed-join-group operations */
case class ConsumerKey(groupId: String, consumerId: String) extends DelayedOperationKey {

  override def keyLabel = "%s-%s".format(groupId, consumerId)
}

/* used by delayed-rebalance operations */
case class ConsumerGroupKey(groupId: String) extends DelayedOperationKey {

  override def keyLabel = groupId
}

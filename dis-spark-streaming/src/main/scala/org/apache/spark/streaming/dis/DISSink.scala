/*
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
package org.apache.spark.streaming.dis

import java.util.concurrent.Future

import com.huaweicloud.dis.adapter.kafka.clients.producer.{DISKafkaProducer, ProducerRecord, RecordMetadata}

class DISSink[K, V](createProducer: () => DISKafkaProducer[K, V]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))

  def send(topic: String, partition: Integer, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, partition, key, value))
}

object DISSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): DISSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new DISKafkaProducer[K, V](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new DISSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): DISSink[K, V] = apply(config.toMap)
}
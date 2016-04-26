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

package org.apache.spark

/**
  * #LIANG-INFO: RDD作为数据结构，本质上是一个只读的分区记录集合。
  * #LIANG-INFO: 一个RDD可以包含多个分区，每个分区就是一个dataset片段。
  * #LIANG-INFO: RDD可以相互依赖。
  * #LIANG-INFO: 如果RDD的每个分区最多只能被一个Child RDD的一个分区使用，则称之为narrow dependency；
  * #LIANG-INFO: 若多个Child RDD分区都可以依赖，则称之为wide dependency。
  * #LIANG-INFO: 不同的操作依据其特性，可能会产生不同的依赖。
  * #LIANG-INFO: 例如map操作会产生narrow dependency，而join操作则产生wide dependency。
  * #LIANG-INFO: 一个RDD通过getPartitions方法得到的是其依赖的父RDD的分区列表.
  * An identifier for a partition in an RDD.
  */
trait Partition extends Serializable {
  /**
    * Get the partition's index within its parent RDD
    */
  def index: Int

  /**
    * #LIANG-INFO: haseCode方法继承自Object类, Partition的子类根据需要可以提供各自的实现
    * #LIANG-INFO: PartitionerAwareUnionRDDPartition子类
    * #LIANG-INFO: HadoopPartition子类
    * #LIANG-INFO: ShuffledRDDPartition子类
    * #LIANG-INFO: NewHadoopPartition子类
    * #LIANG-INFO: ParallelCollectionPartition子类
    * #LIANG-INFO: CoGroupPartition子类
    * #LIANG-INFO: 以上Partiton的子类都自定义了自己的hashCode方法
    *
    * @return
    */
  // A better default implementation of HashCode
  override def hashCode(): Int = index
}

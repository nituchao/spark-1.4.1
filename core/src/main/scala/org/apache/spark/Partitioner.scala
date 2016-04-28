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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.{XORShiftRandom, SamplingUtils}

/**
  * An object that defines how the elements in a key-value pair RDD are partitioned by key.
  * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
  *
  * #LIANG-INFO: 在Spark中分区器直接决定了RDD中分区的个数.
  * #LIANG-INFO: 在Spark中分区器直接决定了每条数据经过Shuffle过程属于哪个分区.
  * #LIANG-INFO: 在Spark中分区器决定了Reduce的个数.
  *
  * #LIANG-INFO: 只有Key-Value类型的RDD才有分区, 非Key-Value类型的RDD分区的值是None.
  * #LIANG-QUESTION: 哪些RDD是非Key-Value型的.
  *
  *
  * #LIANG-INFO: 分区器, 该对象决定了key-value型的RDD是如何按照key进行分区的.
  * #LIANG-INFO: 将key映射成一个分区ID, 分区ID的范围从0到`numPartitions - 1`.
  * #LIANG-INFO: 常用的分区实现: HashPartitioner 和 RangePartitioner.
  */
abstract class Partitioner extends Serializable {
  def numPartitions: Int

  def getPartition(key: Any): Int
}

object Partitioner {
  /**
    * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
    * #LIANG-INFO: 对于成组的RDD, 这些RDD中如果有现成的分区器(partitioner), 则选择一个作为默认的分区器(partitioner)
    * #LIANG-INFO: 否则, 选择HashPartitioner作为默认分区器.
    * #LIANG-INFO: 分区器决定了, 分区数量的决定是必须的, 分区数量有两个参数作为参考,
    * #LIANG-INFO: 如果设置了`spark.default.parallelism`参数, 则优先用该参数作为分区数量.
    * #LIANG-INFO: 否则, 分区数量由最大的那个upstream RDD的upstream partition数量决定.
    *
    * If any of the RDDs already has a partitioner, choose that one.
    *
    * Otherwise, we use a default HashPartitioner. For the number of partitions, if
    * spark.default.parallelism is set, then we'll use the value from SparkContext
    * defaultParallelism, otherwise we'll use the max number of upstream partitions.
    *
    * Unless spark.default.parallelism is set, the number of partitions will be the
    * same as the number of partitions in the largest upstream RDD, as this should
    * be least likely to cause out-of-memory errors.
    *
    *
    * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
    * #LIANG-INFO: HashPartitioner分区器至少有1个RDD, 分区数至少为1
    * #LIANG-INFO: RangePartitioner分区器面向的RDD集合可能为0, 故分区数可能为0
    * #LINAG-INFO: 因为defaultPartitioner有两个参数,rdd和others,甚至这两个参数都为空也会默认用HashPartitiner分区器,故分区数大于0.
    */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse
    for (r <- bySize if r.partitioner.isDefined) {
      return r.partitioner.get
    }
    if (rdd.context.conf.contains("spark.default.parallelism")) {
      new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      new HashPartitioner(bySize.head.partitions.size)
    }
  }
}

/**
  * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using
  * Java's `Object.hashCode`.
  *
  * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
  * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
  * produce an unexpected or incorrect result.
  *
  * #LIANG-INFO: 用key的hashCode对分区数量取模, 将取模结果作为getPartition的返回值.
  *
  * #LIANG-INFO: 使用HashPartitioner分区器要指定分区数量.
  */
class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

/**
  * #LIANG-INFO: RangePartitioner分区尽量保证每个分区中数据量的`均匀`, 而且分区与分区之间是有序的.
  * #LIANG-INFO: 也就是说,一个分区中的元素肯定都比另一个分区内的元素小或者大.
  * #LIANG-INFO: 但是分区内的元素是不能保证顺序的.
  * #LIANG-INFO: 简单的说就是将一定范围内的数据映射到某一个分区内.
  *
  * #LIANG-INFO: RangePartitioner分区器的主要作用就是将一定范围内的数映射到某一个分区内, 故它的实现中分界的算法尤为重要.
  * #LIANG-INFO: RangePartitioner分区器为了确定分区的分界, 采用了Reservoir Sampling(水塘抽样)算法.
  * #LIANG-INFO: Reservoir Sampling算法解决了从n个数中随机取出k个数的问题.
  *
  * #LIANG-INFO: 使用RangePartitioner分区器要指定分区数量.
  * #LIANG-INFO: 使用RangePartitioner分区器要指定Key-Value型RDD对象.
  * #LIANG-INFO: 使用RangePartitiner分区器要指定排序顺序ascending(升序/降序).
  *
  * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
  * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
  *
  * Note that the actual number of partitions created by the RangePartitioner might not be the same
  * as the `partitions` parameter, in the case where the number of sampled records is less than
  * the value of `partitions`.
  */
class RangePartitioner[K: Ordering : ClassTag, V](
                                                   @transient partitions: Int,
                                                   @transient rdd: RDD[_ <: Product2[K, V]],
                                                   private var ascending: Boolean = true)
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]

  /**
    * #LIANG-INFO: rangeBounds是RangePartitioner划分分区的核心算法.
    * #LIANG-INFO: rangeBounds是一个变量.
    * #LIANG-INFO: rangeBounds里保存的是分区的`分界`数组.
    */
  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[K] = {
    /**
      * 构造用于分区的`分界`数组.
      */
    if (partitions <= 1) {
      /**
        * #LIANG-INFO: 如果`0 <= partitions <= 1`, 则返回空的`分界`数组.
        * #LIANG-INFO: 从逻辑上讲, 当分区数为0时, 表示不用分区.
        * #LIANG-INFO: 从逻辑上讲, 当分区数为1时, 表示RDD中的所有Key-Value元素都在同一个分区中.
        */
      Array.empty
    } else {
      /**
        * #LIANG-INFO: 采用ReservoirSample(水塘采样法)进行采样生成`分界`数组.
        * #LIANG-INFO: 采样大小sampleSize默认为partitions(分区数)的20倍, 最大100W.
        */
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      /**
        * #LIANG-INFO: 此处的rdd.partitions是继承自父RDD的方法, 得到父RDD的partiontion数组.
        * #LIANG-INFO: 用父RDD的partition数量作为参考, 得到当前RDD每个分区要抽样的样本数量.
        * #LIANG-INFO: 由于父RDD各分区中的数量可能会出现倾斜的情况, 因此每个分区需要采样的数据量是正常数的3倍.
        * #LIANG-INFO: 乘以3的目的就是保证数据量小的分区能够采样到足够的数据, 而对于数据量大的分区会进行二次采样.
        *
        * #LIANG-INFO: java.lang.Math.ceil(double a) 返回最小的（最接近负无穷大）double值，大于或相等于参数，并相等于一个整数.
        * #LIANG-INFO: eg. Math.ceil(125.9) = 126.0; Math.ceil(0.4873) = 1.0; Math.ceil(-0.65) = -0.0;
        *
        */
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.size).toInt
      /**
        * #LIANG-INFO: 每个分区要采样的样本数量确定以后, 使用水塘采样法对rdd中的元素进行采样.
        * #LIANG-INFO: 对rdd进行采样时对元素的key进行操作, `_._1`即为元素的key.
        * #LIANG-INFO: numItems是rdd元素的总个数.
        * #LIANG-INFO: sketched的类型是`Array[(Int, Int, Array[K])]`, 记录的是分区的编号, 该分区中总元素的个数, 以及从父RDD中每个分区采样的数据.
        */
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.size).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, partitions)
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition - 1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

private[spark] object RangePartitioner {

  /**
    * #LIANG-INFO: sketch 方法用于数据的采样.
    * #LIANG-INFO: 返回结果中:
    * #LIANG-INFO: numItems 表示 RDD 所有数据的个数（等价于之前的 rddSize).
    * #LIANG-INFO: sketched 是一个迭代器，每个元素是一个三元组 (idx, n, sample), 其中 idx 是分区编号, n 是分区的数据个数（而不是采样个数, sample 是一个数组，存储该分区采样得到的样本数据。
    *
    * Sketches the input RDD via reservoir sampling on each partition.
    *
    * @param rdd                    the input RDD to sketch
    * @param sampleSizePerPartition max sample size per partition
    * @return (total number of items, an array of (partitionId, number of items, sample))
    */
  def sketch[K: ClassTag](
                           rdd: RDD[K],
                           sampleSizePerPartition: Int): (Long, Array[(Int, Int, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2.toLong).sum
    (numItems, sketched)
  }

  /**
    * Determines the bounds for range partitioning from candidates with weights indicating how many
    * items each represents. Usually this is 1 over the probability used to sample this candidate.
    *
    * @param candidates unordered candidates with weights
    * @param partitions number of partitions
    * @return selected bounds
    */
  def determineBounds[K: Ordering : ClassTag](
                                               candidates: ArrayBuffer[(K, Float)],
                                               partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight > target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.network

import java.math.BigInteger
import java.net.{InetAddress, UnknownHostException}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._

object IpFilter {
  //Can move most of this to configDef if/when that happens
  val AllowRule = "allow"
  val AllowRuleDescription = "IP not in whitelist"
  val DenyRule = "deny"
  val DenyRuleDescription = "IP in Blacklist"
  val NoRule = "none"
}

class IpFilter(ipFilterList: List[String], ipFilterRuleType: String) extends Logging with KafkaMetricsGroup {
  import IpFilter._
  val ruleType = ipFilterRuleType
  //val ruleType = AllowRule
  val filterList: List[CIDRRange] = {
    try {
      ipFilterList.map(entry => new CIDRRange(entry)).toList
    }
    catch {
      // The exception will be checked when loading properties
      case e: UnknownHostException => throw new IllegalArgumentException("Error processing IP filter List, unable to parse values into CIDR range. ")
    }
  }

  def check(inetAddress: InetAddress): Boolean = {
    val ip: BigInt = new BigInteger(1, inetAddress.getAddress())
    ruleType match {
      case AllowRule => {
        if (filterList.exists(_.contains(ip)))
          true
        else throw new IpFilterException(inetAddress, AllowRuleDescription)
      }
      case DenyRule => {
        if (filterList.exists(_.contains(ip)))
          throw new IpFilterException(inetAddress, DenyRuleDescription)
        else true
      }
      case _ => true
    }
  }
}
/* This class will parse an IP range string (eg. 192.168.2.1/28) and convert it
 * into the lower and upper bound integer values for addresses within that range
 * We use the sign-magnitude representation of the address as a BigInt to support IPV6 addresses.
 * We use bit shifting to apply the mask and get the boundaries for the calculations
 * http://en.wikipedia.org/wiki/Signed_number_representations
 * http://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation
 */
class CIDRRange(val ipRange: String) {

  if (ipRange.indexOf("/") < 0) {
    throw new UnknownHostException("Not a valid CIDR Range " + ipRange)
  }
  val inetAddress = {
    InetAddress.getByName(ipRange.split("/").apply(0))
  }
  private val prefixLen = inetAddress.getAddress.length

  val prefix = ipRange.split("/").apply(1).toInt

  val mask = getMask(prefix, prefixLen)

  /* bitwise "and" the mask for the low address. bitwise "add" the flipped mask for high address */
  val low: BigInt = new BigInt(new BigInteger(1, inetAddress.getAddress)).&(mask)
  val high: BigInt = low.+(~mask)

  /*  match for IPV4 or IPV6 (4 or 16)
    * fill a BigInteger with all ones (-1 = 11111111) for each octet , flip the bits with not()
    * bit-shift right by the length of the prefix
    */
  def getMask(prefix: Int, prefixLen: Int): BigInt = {
    if ((prefix < 0 || prefix > 128) || (prefix > 32 && prefixLen == 4)) {
      throw new UnknownHostException("Not a valid prefix length " + prefix)
    }
    prefixLen match {
      case x if x == 4 => new BigInteger(1, Array.fill[Byte](4)(-1)).not().shiftRight(prefix)
      case x if x == 16 => new BigInteger(1, Array.fill[Byte](16)(-1)).not().shiftRight(prefix)
    }
  }

  def contains(ip: BigInt): Boolean = {
    ip >= low && ip <= high
  }
}

class IpFilterException(val ip: InetAddress, val ruleTypeDescription: String) extends KafkaException("Rejected connection from %s due to IP filter rules (%s)".format(ip, ruleTypeDescription))



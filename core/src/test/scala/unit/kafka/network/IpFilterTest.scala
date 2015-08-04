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
package unit.kafka.network

import java.math.BigInteger
import java.net._
import kafka.network.{CIDRRange, IpFilterException, IpFilter}
import org.junit.Assert._
import org.junit.Test
import kafka.utils.CoreUtils

class IpFilterTest {

  def getBigInt (ipString: String):BigInt = {
    val ipBigInt: BigInt = new BigInteger(1, InetAddress.getByName(ipString).getAddress)
    ipBigInt
  }
  @Test
  def testEmptyList(): Unit = {
    val range = CoreUtils.parseCsvList("").toList
    val ipFilters = new IpFilter(range, IpFilter.NoRule)
    val ip = InetAddress.getByName("192.168.2.1")
    assertTrue(ipFilters.check(ip))
  }

  @Test
  def testBadPrefix: Unit = {
    val range = List(List("192.168.2.1/-1"), List("192.168.2.1/64"))
    for (l <- range) {
      try {
        new IpFilter(l, IpFilter.AllowRule)
        fail()
      } catch {
        case e: IllegalArgumentException =>
      }
    }
  }

  @Test
  // Checks that the IP address is in a given range
  def testIpV4Range(): Unit = {
    val ipRange: String = "192.168.2.0/25" // 0-127
    val cidr = new CIDRRange(ipRange)
    val ip1 = getBigInt("192.168.2.1")
    val ip2 = getBigInt("192.168.2.128")
    assertTrue(cidr.contains(ip1))
    assertFalse(cidr.contains(ip2))
  }

  @Test
  def testIpV6Range1(): Unit = {
    val ipRange: String = "fe80:0:0:0:202:b3ff:fe1e:8320/124"
    val cidr = new CIDRRange(ipRange)
    val ip1 = getBigInt("fe80:0:0:0:202:b3ff:fe1e:8320")
    val ip2 = getBigInt("fe80:0:0:0:202:b3ff:fe1e:833f")
    assertTrue(cidr.contains(ip1))
    assertFalse(cidr.contains(ip2))
  }

  @Test
  def testIPV6Range2(): Unit = {
    val ipRange: String = "fe80:0:0:0:202:b3ff:fe1e:8320/64"
    val cidr = new CIDRRange(ipRange)
    val ip1 = getBigInt("fe80:0000:0000:0000:0202:b3ff:fe1e:8320")
    val ip2 = getBigInt("fe80:0000:0000:0000:ffff:ffff:ffff:ffff")
    val ip3 = getBigInt("fe80:0:0:3:ffff:ffff:ffff:ffff")
    assertTrue(cidr.contains(ip1))
    assertTrue(cidr.contains(ip2))
    assertFalse(cidr.contains(ip3))
  }

  // This is kind of a bogus test but tests the logic for ranges < 32 and IPv6
  @Test
  def testIPV6Range3(): Unit = {
    val ipRange: String = "fe80:0:0:0:202:b3ff:fe1e:8320/12"
    val cidr = new CIDRRange(ipRange)
    val ip1 = getBigInt("fe80:0000:0000:0000:0202:b3ff:fe1e:8320")
    val ip2 = getBigInt("fe8f:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
    val ip3 = getBigInt("fe9f:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
    assertTrue(cidr.contains(ip1))
    assertTrue(cidr.contains(ip2))
    assertFalse(cidr.contains(ip3))
  }

  @Test
  def testSingleBlackList(): Unit = {
    val range1 = List("192.168.2.10/32")
    val ipFilters = new IpFilter(range1, IpFilter.DenyRule)
    val ip = InetAddress.getByName("192.168.2.10")
    try {
      ipFilters.check(ip)
      fail()
    } catch {
      case e: IpFilterException => // this is good
    }
    val ip2 = InetAddress.getByName("192.168.2.1")
    assertTrue(ipFilters.check(ip2))
  }

  @Test
  def testMultipleBlackListEntries(): Unit = {
    val range = List("192.168.2.0/28", "192.168.2.25/28")
    val ipFilters = new IpFilter(range, IpFilter.DenyRule)
    val ip1 = InetAddress.getByName("192.168.2.3")
    val ip2 = InetAddress.getByName("192.168.2.26")
    val ip3 = InetAddress.getByName("192.162.1.1")
    try {
      ipFilters.check(ip1)
      fail()
    } catch {
      case e: IpFilterException => // this is good
    }
    try {
      ipFilters.check(ip2)
      fail()
    } catch {
      case e: IpFilterException => // this is good
    }
    assertTrue(ipFilters.check(ip3))
  }

  @Test
  def testSingleWhiteList(): Unit = {
    val range1 = List("192.168.2.10/32")
    val ipFilters = new IpFilter(range1, IpFilter.AllowRule)
    val ip = InetAddress.getByName("192.168.2.10")
    val ip2 = InetAddress.getByName("192.168.2.1")
    assertTrue(ipFilters.check(ip))
    try {
      ipFilters.check(ip2)
      fail()
    } catch {
      case e: IpFilterException => // this is good
    }
  }

  @Test
  def testMultipleWhiteListEntries(): Unit = {
    val range = List("192.168.2.0/24", "10.10.10.0/16")
    val ipFilters = new IpFilter(range, IpFilter.AllowRule)
    val ip1 = InetAddress.getByName("192.168.2.128")
    val ip2 = InetAddress.getByName("192.168.1.128")
    val ip3 = InetAddress.getByName("10.10.1.1")
    val ip4 = InetAddress.getByName("10.9.1.1")
    assertTrue(ipFilters.check(ip1))
    try {
      ipFilters.check(ip2)
      fail()
    } catch {
      case e: IpFilterException => // this is good
    }
    assertTrue(ipFilters.check(ip3))
    try {
      ipFilters.check(ip4)
      fail()
    } catch {
      case e: IpFilterException => // this is good
    }
  }

  @Test
  def testRangeFormat(): Unit = {
    val ruleType = IpFilter.AllowRule
    val range1 = List("192.168.2")
    val range2 = List("192.168.1.2/32", "10.A.B.C/AAAA")
    val range3 = List("blahblahblah:")
    val range4 = List("192aaaa:")
    val rangeList: List[List[String]] = List(range1, range2, range2, range4)

    for (l <- rangeList)
      try {
        val ipFilters = new IpFilter(l, ruleType)
        fail()
      } catch {
        case e: IllegalArgumentException =>
      }
  }
}



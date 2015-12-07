package com.barclays.adacore

import com.barclays.adacore.anonymizers._
import com.barclays.adacore.utils.{VPTree, TestConfig, TestUtils}
import org.specs2.mutable.Specification

class AccountAnonymizerSpec extends Specification with TestUtils with TestConfig {
  sequential

  import AccountAnonymizer._

  "AccountAnonymizer" should {
    val testRawAccounts1stGroup@List(testRawAccount1, testRawAccount2, testRawAccount3) = List(
      RawAccount(1523869, None, Some(102), false, Some(0.0), 3692.0, Some("IG75"), 8, "F", Some(5), None),
      RawAccount(1521962, Some(0.0), Some(84), false, None, 0.0, Some("IG50"), 28, "F", Some(2), None),
      RawAccount(1510221, Some(0.0), Some(61), false, None, 26000.0, Some("IG62"), 29, "M", Some(2), None)
    )

    // group of raw accounts sharing the same categories bucket
    val testRawAccounts2ndGroup@List(testRawAccount4, testRawAccount5, testRawAccount6,
    testRawAccount7) = List(
      RawAccount(1513282, Some(8.0), Some(60), false, Some(25769.8), 12000.0, Some("IG76"), 26, "M", Some(2), Some(4)),
      RawAccount(1513283, Some(9.0), Some(60), false, Some(25769.8), 12000.0, Some("IG76"), 26, "M", Some(2), Some(4)),
      RawAccount(1513284, Some(10.0), Some(60), false, Some(25769.8), 12000.0, Some("IG76"), 26, "M", Some(2), Some(4)),
      RawAccount(1513285, Some(11.0), Some(60), false, Some(25769.8), 12000.0, Some("IG76"), 26, "M", Some(2), Some(4))
    )

    val testRawAccounts3rdGroup@List(testRawAccount8, testRawAccount9, testRawAccount10) = List(
      RawAccount(1513274, Some(0.0), Some(50), false, None, 0.0, Some("IG14"), 25, "M", None, Some(12)),
      RawAccount(1512441, Some(1593.0), Some(47), false, Some(0.0), 0.0, Some("IG45"), 25, "F", Some(2), None),
      RawAccount(1517788, Some(0.0), Some(44), false, None, 0.0, Some("IG14"), 53, "F", Some(2), Some(15))
    )

    val testRawAccounts4thGroup@List(testRawAccount11, testRawAccount12) = List(
      RawAccount(1518794, Some(6034.0), Some(86), true, Some(132447.0), 23000.0, Some("SL97"), 3, "M", Some(5), None),
      RawAccount(1512615, Some(0.0), Some(44), false, None, 0.0, Some("IG39"), 25, "F", Some(2), None)
    )

    val testRawAccount13 = RawAccount(1512912, None, Some(50), false, None, 26000.0, Some("IG13"), 25, "M", Some(2), None)
    //  val testRawAccount14 = RawAccount(1514413, Some(5.0), Some(65), false, Some(0.0), 8728.0, Some("RM65"), 30, "F", None, Some(1)),


    "correctly anonymize an rdd of raw account" in {

      "with k = 3" in {
        "and an empty rdd of accounts" in {
          AccountAnonymizer.generalizeByCategoricalBuckets(sc, sc.parallelize[RawAccount](Nil), 3).collect() must_=== Array.empty
        }

        "and throw an exception when there is only one account" in {
          AccountAnonymizer.generalizeByCategoricalBuckets(sc, sc.parallelize[RawAccount](List(testRawAccount1)), 3).collect() must
            throwA[IllegalArgumentException]
        }

        "and throw an exception when there are two accounts" in {
          AccountAnonymizer.generalizeByCategoricalBuckets(sc, sc.parallelize[RawAccount](List(testRawAccount1, testRawAccount2)), 3)
          .collect() must throwA[IllegalArgumentException]
        }

        "and exactly 3 accounts of different buckets" in {
          val testCategoryGroup = testRawAccounts1stGroup.map(_.categoricalBucket).toSet

          "at the generalize method" in {
            AccountAnonymizer.generalizeByCategoricalBuckets(sc, sc.parallelize[RawAccount](testRawAccounts1stGroup), 3).keys.collect().toSet must_===
              testRawAccounts1stGroup.map(ra => GeneralizedCategoricalBucketGroup(testCategoryGroup, 3)).toSet
          }

          "at categories of small buckets" in {
            val testVPTree = VPTree(testCategoryGroup.map(_ -> 1).toArray, AccountAnonymizer.bucketsDistance, 1)
            AccountAnonymizer.mergeBuckets(testCategoryGroup.map(_ -> 1).toMap, testVPTree, 3, Set.empty) must_===
              List(testCategoryGroup -> 3)
          }
        }

        "and 10 accounts of which 4 are in the same bucket" in {
          AccountAnonymizer.generalizeByCategoricalBuckets(sc, sc.parallelize[RawAccount](testRawAccounts1stGroup ++
            testRawAccounts2ndGroup ++ testRawAccounts3rdGroup), 3).keys.distinct().collect().toSet must_===
            Set(
              GeneralizedCategoricalBucketGroup(Set(testRawAccounts2ndGroup.head.categoricalBucket), testRawAccounts2ndGroup.size),
              GeneralizedCategoricalBucketGroup(Set(testRawAccount10, testRawAccount1, testRawAccount8).map(_.categoricalBucket), 3),
              GeneralizedCategoricalBucketGroup(Set(testRawAccount2, testRawAccount3, testRawAccount9).map(_.categoricalBucket), 3)
            )
        }

        "and 12 accounts of which 4 are in the same bucket" in {
          //          Set(GeneralizedCategoricalBucketGroup(
          // Set(
          // CategoricalBucket(false,29,M,Some(2),None), 3
          // CategoricalBucket(false,25,F,Some(2),None), 9-12
          // CategoricalBucket(false,25,M,None,Some(12)), 8
          // CategoricalBucket(true,3,M,Some(5),None)),5), 11

          // GeneralizedCategoricalBucketGroup(Set(CategoricalBucket(false,26,M,Some(2),Some(4))),4),
          //
          // GeneralizedCategoricalBucketGroup(
          // Set(CategoricalBucket(false,28,F,Some(2),None), 2
          // CategoricalBucket(false,53,F,Some(2),Some(15)), 10
          // CategoricalBucket(false,8,F,Some(5),None)),3)) 1

          AccountAnonymizer.generalizeByCategoricalBuckets(sc, sc.parallelize[RawAccount](testRawAccounts1stGroup ++
            testRawAccounts2ndGroup ++ testRawAccounts3rdGroup ++
            testRawAccounts4thGroup), 3).keys.distinct().collect().toSet must_===
            Set(
              GeneralizedCategoricalBucketGroup(Set(testRawAccounts2ndGroup.head.categoricalBucket), 4),
              GeneralizedCategoricalBucketGroup(Set(testRawAccount9, testRawAccount3, testRawAccount8, testRawAccount11,
                testRawAccount12)
                                                .map(_.categoricalBucket), 5),
              GeneralizedCategoricalBucketGroup(
                Set(testRawAccount1, testRawAccount2, testRawAccount10)
                .map(_.categoricalBucket), 3
              )
            )
        }

        "VP tree returns always the same results" in {
          val data = Array(
            GeneralizedCategoricalBucketGroup(Set(testRawAccounts2ndGroup.head.categoricalBucket), 4),
            GeneralizedCategoricalBucketGroup(Set(testRawAccount10, testRawAccount8, testRawAccount1)
                                              .map(_.categoricalBucket), 3),
            GeneralizedCategoricalBucketGroup(
              Set(testRawAccount2, testRawAccount3, testRawAccount9, testRawAccount11, testRawAccount12)
              .map(_.categoricalBucket), 5
            )
          ).flatMap(group => group.group.map(_ -> group.groupSize))

          (for {
            _ <- (1 to 1000)
            maxDist <- (1 to 5)
            (bucket, _) <- data
          } yield (bucket, maxDist) -> VPTree(data, AccountAnonymizer.bucketsDistance, 1).nearest(bucket, maxDist).toSet
            ).groupBy(_._1).mapValues(_.map(_._2).toSet).forall(_._2.size must_=== 1)
        }
      }

      /*"correctly merge numerical buckets" in {
        "with 1 bucket and k = 3" in {
          val bucket1 = NumericalBucket(10, 25, 10000, 40000)

          val buckets = List(bucket1)

          AccountAnonymizer.generalizeByNumericalBuckets(buckets, 3) must_===
            Set(GeneralizedNumericalBucketGroup(LongRange(10, 10), LongRange(25, 25), LongRange(10000, 10000),
              LongRange(40000, 40000), 1))
        }

        "with 3 buckets and k = 3" in {
          val bucket1 = NumericalBucket(10, 25, 10, 40)
          val bucket2 = NumericalBucket(12, 20, 10, 20)
          val bucket3 = NumericalBucket(6, 20, 10, 30)

          val buckets = List(bucket1, bucket2, bucket3)

          AccountAnonymizer.generalizeByNumericalBuckets(buckets, 3) must_===
            Set(GeneralizedNumericalBucketGroup(LongRange(6, 12), LongRange(20, 25), LongRange(10, 10),
              LongRange(20, 40), 3))
        }

        "with 4 different buckets and k = 3" in {
          val bucket1 = NumericalBucket(10, 25, 10, 40)
          val bucket2 = NumericalBucket(12, 20, 10, 20)
          val bucket3 = NumericalBucket(6, 20, 10, 30)
          val bucket4 = NumericalBucket(8, 20, 10, 80)

          val buckets = List(bucket1, bucket2, bucket3, bucket4)

          AccountAnonymizer.generalizeByNumericalBuckets(buckets, 3) must_===
            Set(GeneralizedNumericalBucketGroup(LongRange(6, 12), LongRange(20, 25), LongRange(10, 10),
              LongRange(20, 40), 3))
        }

        "with 10 buckets of repeated values and k = 3" in {
          val bucket1 = NumericalBucket(10, 25, 10, 40)
          val bucket2 = NumericalBucket(12, 22, 12, 20)
          val bucket3 = NumericalBucket(6, 20, 14, 30)
          val bucket4 = NumericalBucket(8, 18, 16, 80)

          val buckets = List(bucket1, bucket1, bucket1, bucket2, bucket2, bucket2, bucket3, bucket3, bucket3, bucket4)

          AccountAnonymizer.generalizeByNumericalBuckets(buckets, 3) must_===
            Set(
              GeneralizedNumericalBucketGroup(LongRange(10, 10), LongRange(25, 25), LongRange(10, 10), LongRange(40, 40), 3),
              GeneralizedNumericalBucketGroup(LongRange(12, 12), LongRange(20, 20), LongRange(10, 10), LongRange(20, 20), 3),
              GeneralizedNumericalBucketGroup(LongRange(6, 6), LongRange(20, 20), LongRange(10, 10), LongRange(30, 30), 3)
            )
        }

        "with 20 buckets of repeated values and k = 3" in {
          val bucket1 = NumericalBucket(10, 25, 10, 40)
          val bucket2 = NumericalBucket(12, 20, 10, 20)
          val bucket3 = NumericalBucket(6, 20, 10, 30)
          val bucket4 = NumericalBucket(8, 20, 10, 80)

          val buckets = Array.fill(6)(bucket1) ++ Array.fill(4)(bucket2) ++ Array.fill(8)(bucket3) ++ Array.fill(2)(bucket4)

          AccountAnonymizer.generalizeByNumericalBuckets(buckets, 3) must_===
            Set(
              GeneralizedNumericalBucketGroup(LongRange(10, 10), LongRange(25, 25), LongRange(10, 10), LongRange(40, 40), 3),
              GeneralizedNumericalBucketGroup(LongRange(12, 12), LongRange(20, 20), LongRange(10, 10), LongRange(20, 20), 3),
              GeneralizedNumericalBucketGroup(LongRange(6, 6), LongRange(20, 20), LongRange(10, 10), LongRange(30, 30), 3)
            )
        }
      }*/
    }
  }
}

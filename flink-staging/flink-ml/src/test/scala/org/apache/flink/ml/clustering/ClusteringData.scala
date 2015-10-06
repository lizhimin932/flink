/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.clustering

import breeze.linalg.{DenseVector => BreezeDenseVector, Vector => BreezeVector}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{DenseVector, Vector}

/**
 * Trainings- and test-data set for the K-Means implementation
 * [[org.apache.flink.ml.clustering.KMeans]].
 */
object ClusteringData {

  /*
   * Number of iterations for the K-Means algorithm.
   */
  val iterations = 10

  /*
   * Sequence of initial centroids.
   */
  val centroidData: Seq[LabeledVector] = Seq(
    LabeledVector(1, DenseVector(-0.1369104662767052, 0.2949172396037093, -0.01070450818187003)),
    LabeledVector(2, DenseVector(0.43643950041582885, 0.30117329671833215, 0.20965108353159922)),
    LabeledVector(3, DenseVector(0.26011627041438423, 0.22954649683337805, 0.2936286262276151)),
    LabeledVector(4, DenseVector(-0.041980932305508145, 0.03116256923634109, 0.31065743174542293)),
    LabeledVector(5, DenseVector(0.0984398491976613, -0.21227718242541602, -0.45083084300074255)),
    LabeledVector(6, DenseVector(-0.2165269235451111, -0.47142840804338293, -0.02298954070830948)),
    LabeledVector(7, DenseVector(-0.0632307695567563, 0.2387221400443612, 0.09416850805771804)),
    LabeledVector(8, DenseVector(0.16383680898916775, -0.24586810465119346, 0.08783590589294081)),
    LabeledVector(9, DenseVector(-0.24763544645492513, 0.19688995732231254, 0.4520904742796472)),
    LabeledVector(10, DenseVector(0.16468044138881932, 0.06259522206982082, 0.12145870313604247))

  )

  /*
   * 3 Dimensional DenseVectors from a Part of Cosmo-Gas Dataset
   * Reference: http://nuage.cs.washington.edu/benchmark/
   */
  val trainingData: Seq[Vector] = Seq(
    DenseVector(-0.489811986685, 0.496883004904, -0.483860999346),
    DenseVector(-0.485296010971, 0.496421992779, -0.484212994576),
    DenseVector(-0.481514006853, 0.496134012938, -0.48508900404),
    DenseVector(-0.478542000055, 0.496246010065, -0.486301004887),
    DenseVector(-0.475461006165, 0.496093004942, -0.487686008215),
    DenseVector(-0.471846997738, 0.496558994055, -0.488242000341),
    DenseVector(-0.467496991158, 0.497166007757, -0.48861899972),
    DenseVector(-0.463036000729, 0.497680991888, -0.489721000195),
    DenseVector(-0.458972990513, 0.4984369874, -0.490575999022),
    DenseVector(-0.455772012472, 0.499684005976, -0.491737008095),
    DenseVector(-0.453074991703, -0.499433010817, -0.492006987333),
    DenseVector(-0.450913995504, -0.499316990376, -0.492769002914),
    DenseVector(-0.448724985123, -0.499406009912, -0.493508011103),
    DenseVector(-0.44715899229, -0.499680995941, -0.494500011206),
    DenseVector(-0.445362001657, -0.499630987644, -0.495151996613),
    DenseVector(-0.442811012268, -0.499303996563, -0.495151013136),
    DenseVector(-0.439810991287, -0.499332994223, -0.49529799819),
    DenseVector(-0.43678098917, -0.499361991882, -0.49545699358),
    DenseVector(-0.433919012547, -0.499334007502, -0.495705991983),
    DenseVector(-0.43117800355, -0.499345004559, -0.496196985245),
    DenseVector(-0.428333997726, -0.499083012342, -0.496385991573),
    DenseVector(-0.425300985575, -0.49844199419, -0.496405988932),
    DenseVector(-0.421882003546, -0.497743010521, -0.496706992388),
    DenseVector(-0.418137013912, -0.497193992138, -0.496524989605),
    DenseVector(-0.414458990097, -0.496717989445, -0.49600699544),
    DenseVector(-0.411509007215, -0.495965003967, -0.495519012213),
    DenseVector(-0.40851598978, -0.49593898654, -0.495027005672),
    DenseVector(-0.405084013939, -0.497224003077, -0.494318008423),
    DenseVector(-0.402155995369, -0.498420000076, -0.493582010269),
    DenseVector(-0.399185985327, -0.499316990376, -0.493566006422),
    DenseVector(-0.396214991808, -0.499727994204, -0.494017004967),
    DenseVector(-0.393094986677, -0.499821007252, -0.494278013706),
    DenseVector(-0.389335989952, -0.499379009008, -0.494480013847),
    DenseVector(-0.385125994682, -0.499267995358, -0.494628995657),
    DenseVector(-0.380605995655, -0.499545991421, -0.495085000992),
    DenseVector(-0.376213997602, -0.499879002571, -0.495617002249),
    DenseVector(-0.372996985912, 0.499734997749, -0.496517002583),
    DenseVector(-0.368934988976, 0.499749004841, -0.496690988541),
    DenseVector(-0.363835990429, -0.499909996986, -0.496495991945),
    DenseVector(-0.358395010233, 0.49980199337, -0.49607899785),
    DenseVector(-0.353298008442, -0.499940007925, -0.495460003614),
    DenseVector(-0.349240005016, -0.499356001616, -0.494697004557),
    DenseVector(-0.345212012529, -0.499731004238, -0.494096010923),
    DenseVector(-0.341008991003, 0.499749988317, -0.493512988091),
    DenseVector(-0.336104005575, 0.498928010464, -0.49247199297),
    DenseVector(-0.330855995417, 0.498306006193, -0.491232007742),
    DenseVector(-0.32566100359, 0.498154014349, -0.490224003792),
    DenseVector(-0.320849001408, 0.498154014349, -0.489493995905),
    DenseVector(-0.316397994757, 0.49818199873, -0.488979011774),
    DenseVector(-0.311291992664, 0.49848100543, -0.488063007593),
    DenseVector(-0.30513599515, 0.498423010111, -0.487619996071),
    DenseVector(-0.299059003592, 0.498239010572, -0.486963003874),
    DenseVector(-0.295850992203, 0.497961014509, -0.486425995827),
    DenseVector(-0.292504996061, 0.49786400795, -0.486220985651),
    DenseVector(-0.287795990705, 0.496935009956, -0.486378014088),
    DenseVector(-0.282094985247, 0.496926009655, -0.486101001501),
    DenseVector(-0.230370000005, -0.423029005527, -0.190435007215),
    DenseVector(-0.226144999266, -0.422674000263, -0.190456002951),
    DenseVector(-0.221065998077, -0.422462999821, -0.190656006336),
    DenseVector(-0.21570199728, -0.421921014786, -0.190736994147),
    DenseVector(-0.211145997047, -0.421442002058, -0.190715998411),
    DenseVector(-0.207176998258, -0.421692997217, -0.191337004304),
    DenseVector(-0.202617004514, -0.421979010105, -0.192610993981),
    DenseVector(-0.197987005115, -0.421979010105, -0.1939329952),
    DenseVector(-0.193534001708, -0.42171099782, -0.195063993335),
    DenseVector(-0.188442006707, -0.421469986439, -0.195659995079),
    DenseVector(-0.18351200223, -0.421350002289, -0.196327000856),
    DenseVector(-0.178878992796, -0.421176999807, -0.196639999747),
    DenseVector(-0.173997998238, -0.420922011137, -0.19677400589),
    DenseVector(-0.17026899755, -0.420855998993, -0.196733996272),
    DenseVector(-0.166736006737, -0.420551985502, -0.196759000421),
    DenseVector(-0.16250500083, -0.420587986708, -0.19698600471),
    DenseVector(-0.158608004451, -0.420758008957, -0.196684002876),
    DenseVector(-0.154406994581, -0.420715004206, -0.196183994412),
    DenseVector(-0.15014000237, -0.420192986727, -0.1962479949),
    DenseVector(-0.145583003759, -0.419409006834, -0.196958005428),
    DenseVector(-0.141097992659, -0.41882699728, -0.197107002139),
    DenseVector(-0.13644400239, -0.418215990067, -0.196890994906),
    DenseVector(-0.132035002112, -0.417602986097, -0.196718007326),
    DenseVector(-0.128143996, -0.417082995176, -0.196645006537),
    DenseVector(-0.124609999359, -0.416640013456, -0.196575000882),
    DenseVector(-0.12135899812, -0.416545987129, -0.196443006396),
    DenseVector(-0.11831600219, -0.416736006737, -0.196152001619),
    DenseVector(-0.114499002695, -0.416723996401, -0.195667997003),
    DenseVector(-0.110071003437, -0.416583001614, -0.195353999734),
    DenseVector(-0.105696000159, -0.416215986013, -0.195015996695),
    DenseVector(-0.101567000151, -0.415634006262, -0.194840997458),
    DenseVector(-0.0976777970791, -0.415030002594, -0.194831997156),
    DenseVector(-0.0947626978159, -0.414595991373, -0.195255994797),
    DenseVector(-0.0925178974867, -0.414178013802, -0.195669993758),
    DenseVector(-0.0899709016085, -0.413747012615, -0.195713996887),
    DenseVector(-0.0869152024388, -0.413572013378, -0.195683002472),
    DenseVector(-0.0834548026323, -0.413212001324, -0.195618003607),
    DenseVector(-0.0799069032073, -0.412741005421, -0.195555001497),
    DenseVector(-0.0765667036176, -0.412616014481, -0.195696994662),
    DenseVector(-0.0730601996183, -0.412665009499, -0.195963993669),
    DenseVector(-0.0695542991161, -0.412683993578, -0.196098998189),
    DenseVector(-0.0661773011088, -0.412420988083, -0.196201995015),
    DenseVector(-0.062273401767, -0.412048995495, -0.196441993117),
    DenseVector(-0.05775950104, -0.412072986364, -0.196572005749),
    DenseVector(-0.0543152987957, -0.412909001112, -0.196082994342),
    DenseVector(-0.0515625998378, -0.413266003132, -0.195540994406),
    DenseVector(-0.0482833012938, -0.413659006357, -0.195500999689),
    DenseVector(-0.0447212010622, -0.413929998875, -0.195748001337),
    DenseVector(-0.0415252000093, -0.413904994726, -0.195501998067),
    DenseVector(-0.0375672988594, -0.413911998272, -0.194977998734),
    DenseVector(-0.0325004011393, -0.413509994745, -0.194503992796),
    DenseVector(-0.0276785008609, -0.412813007832, -0.194422006607),
    DenseVector(-0.0232041999698, -0.412286996841, -0.194086000323),
    DenseVector(-0.0188629999757, -0.412007004023, -0.193660005927),
    DenseVector(-0.0146391997114, -0.411799997091, -0.193238005042),
    DenseVector(0.438068985939, -0.423878014088, -0.193721994758),
    DenseVector(0.441168993711, -0.423072993755, -0.193834006786),
    DenseVector(0.445264995098, -0.422354012728, -0.193957000971),
    DenseVector(0.449609994888, -0.421038001776, -0.193471997976),
    DenseVector(0.452950000763, -0.419548988342, -0.194122001529),
    DenseVector(0.455969005823, -0.418231010437, -0.19481100142),
    DenseVector(0.458950012922, -0.417178988457, -0.195061996579),
    DenseVector(0.462146013975, -0.416909009218, -0.195247992873),
    DenseVector(0.466147005558, -0.417118012905, -0.195875003934),
    DenseVector(0.470245987177, -0.417659014463, -0.196611002088),
    DenseVector(0.474249005318, -0.41837900877, -0.197458997369),
    DenseVector(0.478522986174, -0.419180989265, -0.197898998857),
    DenseVector(0.482955992222, -0.419600009918, -0.198066994548),
    DenseVector(0.487857013941, -0.419793009758, -0.198157995939),
    DenseVector(0.492332011461, -0.420217990875, -0.198266997933),
    DenseVector(0.49594399333, -0.421918988228, -0.19885699451),
    DenseVector(0.49856698513, -0.421321004629, -0.199806004763),
    DenseVector(-0.49766099453, -0.419916987419, -0.200901001692),
    DenseVector(-0.493865013123, -0.416572988033, -0.201345995069),
    DenseVector(-0.491010010242, -0.417364001274, -0.201638996601),
    DenseVector(-0.488465994596, -0.41782400012, -0.202114000916),
    DenseVector(-0.486595988274, -0.418282985687, -0.202690005302),
    DenseVector(-0.484320014715, -0.418422996998, -0.203033000231),
    DenseVector(-0.481851994991, -0.418476998806, -0.202452003956),
    DenseVector(-0.479981005192, -0.418624013662, -0.201403006911),
    DenseVector(-0.47786000371, -0.418918997049, -0.200885996222),
    DenseVector(-0.476034998894, -0.419658988714, -0.200096994638),
    DenseVector(-0.473533004522, -0.420367002487, -0.199972003698),
    DenseVector(-0.470723986626, -0.421427994967, -0.199258998036),
    DenseVector(-0.467518001795, -0.421974986792, -0.199303001165),
    DenseVector(-0.463970988989, -0.422064006329, -0.199428007007),
    DenseVector(-0.459452986717, -0.422127008438, -0.199609994888),
    DenseVector(-0.45430201292, -0.422224998474, -0.199546992779),
    DenseVector(-0.44893398881, -0.421568006277, -0.199607998133),
    DenseVector(-0.443767011166, -0.421770989895, -0.199814006686),
    DenseVector(-0.438787996769, -0.421896994114, -0.199639007449),
    DenseVector(-0.43403300643, -0.421761006117, -0.199591994286),
    DenseVector(-0.429554998875, -0.421952992678, -0.199662998319),
    DenseVector(-0.425689995289, -0.422435998917, -0.200434997678),
    DenseVector(-0.422215998173, -0.423157989979, -0.20145599544),
    DenseVector(-0.418269991875, -0.423471987247, -0.202366992831),
    DenseVector(-0.414126008749, -0.42369300127, -0.203261002898),
    DenseVector(-0.411013990641, -0.423835992813, -0.203920006752),
    DenseVector(-0.408345997334, -0.423550993204, -0.204453006387),
    DenseVector(-0.406082987785, -0.422883987427, -0.204586997628),
    DenseVector(-0.403436988592, -0.422601014376, -0.204478994012),
    DenseVector(-0.399006009102, -0.423094987869, -0.203880995512),
    DenseVector(-0.39403501153, -0.422764986753, -0.202616006136),
    DenseVector(-0.389073014259, -0.422423005104, -0.201444000006),
    DenseVector(-0.384308993816, -0.422013998032, -0.201012000442),
    DenseVector(-0.379889011383, -0.421714991331, -0.201112002134),
    DenseVector(-0.375250011683, -0.420976996422, -0.201361998916),
    DenseVector(-0.371003001928, -0.420338004827, -0.201533004642),
    DenseVector(-0.366775989532, -0.420608013868, -0.201326996088),
    DenseVector(-0.362919986248, -0.421036988497, -0.20096899569),
    DenseVector(-0.358947008848, -0.421590000391, -0.201083004475)

  )

  /*
   * For reliable checking these expected-vectors are the output of Twister (another iterative
   * framework)
   * Reference: http://www.iterativemapreduce.org/
   */
  val expectedCentroids = Seq[LabeledVector](
    LabeledVector(1, DenseVector(-0.37971876676276917, 0.4979574657403462, -0.4891930004726923)),
    LabeledVector(6, DenseVector(-0.28812266733768305, -0.4380759022409115, -0.2696436452528952)),
    LabeledVector(8, DenseVector(0.46770288137823535, -0.4198470028007058, -0.1961898225195882))
  )

  /*
   * Contains points with their expected label.
   */
  val testData = Seq[LabeledVector](
    LabeledVector(1, DenseVector(-0.37971876676276917, 0.4979574657403462, -0.4891930004726923)),
    LabeledVector(6, DenseVector(-0.28812266733768305, -0.4380759022409115, -0.2696436452528952)),
    LabeledVector(8, DenseVector(0.46770288137823535, -0.4198470028007058, -0.1961898225195882)),
    LabeledVector(1, DenseVector(-0.4, 0.5, -0.5)),
    LabeledVector(6, DenseVector(-0.3, -0.45, -0.27)),
    LabeledVector(8, DenseVector(0.48, -0.42, -0.2)),
    LabeledVector(1, DenseVector(-0.3, 0.47, -0.4)),
    LabeledVector(6, DenseVector(-0.25, -0.4, -0.2)),
    LabeledVector(8, DenseVector(0.5, -0.4, -0.25)),
    LabeledVector(1, DenseVector(-0.28, 0.6, -0.5)),
    LabeledVector(6, DenseVector(-0.2, -0.5, -0.2)),
    LabeledVector(8, DenseVector(0.6, -0.4, -0.1))
  )
}

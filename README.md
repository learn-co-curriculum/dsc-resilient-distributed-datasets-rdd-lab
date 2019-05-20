
## Resilient Distributed Datasets (RDDs) - Lab

Resilient Distributed Datasets (RDD) are fundamental data structures of Spark. An RDD is, essentially, the Spark representation of a set of data, spread across multiple machines, with APIs to let you act on it. An RDD could come from any datasource, e.g. text files, a database, a JSON file etc.


## Objectives

You will be able to:

* Describe RDDs as fundamental storage units in Spark computing environment
* Create RDDs from Python collections
* Set number of partitions for parallelizing RDDs
* Review an RDD's dependancy graph at different stages of processing. 
* Apply the map(func) transformation to a given function on all elements of an RDD in different partitions
* Use collect() action to trigger the processing stage of spark's lazy evaluation
* Use count() action to calculate the number of elements of a parallelized RDD
* Use filter(func) to filter unwanted data from RDDs
* Develop an understanding of Python's lambda functions for RDDs processing


## What are RDDs? 

To get a better understanding of RDDs, let's break down each one of the components of the acronym RDD:

Resilient: RDDs are considered "resilient" because they have built-in fault tolerance. This means that even if one of the nodes goes offline, RDDs will be able to restore the data. This is already a huge advantage compared to standard storage. If a standard computer dies will performing an operation, all of its memory will be lost in the process. With RDDs, multiple nodes can go offline, and the action will still be held in working memory.

Distributed: The data is contained on multiple nodes of a cluster-computing operation. It is efficiently partitioned to allow for parallelism.

Dataset: The dataset has been * partitioned * across the multiple nodes. 

RDDs are the building block upon which more high level spark operations are based upon. Chances are, if you are performing an action using Spark, the operation involves RDDs. 



Key Characteristics of RDDs:

- Immutable: Once an RDD is created, it cannot be modified.
- Lazily Evaluated: RDDs will not be evaluated until an action is triggered. Essentially, when RDDs are created, they are programmed to perform some action, but that function will not get activated until it is explicitly called. The reason for lazy evaluation is that allows users to organize the actions of their Spark program into smaller actions. It also saves unnecessary computation and memory load.
- In-Memory: The operations in Spark are performed in-memory rather than in the Database. This is what allows Spark to perform fast operations with very large quantities of data.




### RDD Transformations vs Actions

In Spark, we first create a __base RDD__ and then apply one or more transformations to that base RDD following our processing needs. Being immutable means, **once an RDD is created, it cannot be changed**. As a result, **each transformation of an RDD creates a new RDD**. Finally, we can apply one or more **actions** to the RDDs. Spark uses lazy evaluation, so transformations are not actually executed until an action occurs.


<img src="rdd1.png" width=500>

### Transformations

Transformations create a new data set from an existing one by passing each dataset element through a function and returning a new RDD representing the results. In short, creating an RDD from an existing RDD is ‘transformation’.
All transformations in Spark are lazy. They do not compute their results right away. Instead, they just remember the transformations applied to some base data set (e.g. a file). The transformations are only computed when an action requires a result that needs to be returned to the driver program.
A transformation a RDD that returns another RDD, like map, flatMap, filter, reduceByKey, join, cogroup, etc.

### Actions
Actions return final results of RDD computations. Actions trigger execution using lineage graph to load the data into original RDD and carry out all intermediate transformations and return the final results to the Driver program or writes it out to the file system. An action returns a value (to a Spark driver - the user program).

Here are some of key transformations and actions that we will explore.


| Transformations   | Actions       |
|-------------------|---------------|
| map(func)         | reduce(func)  |
| filter(func)      | collect()     |
| groupByKey()      | count()       |
| reduceByKey(func) | first()       |
| mapValues(func)   | take()        |
| sample()          | countByKey()  |
| distinct()        | foreach(func) |
| sortByKey()       |               |


Let's see how transformations and actions work through a simple example. In this example, we will perform several actions and transformations on RDDs in order to obtain a better understanding of Spark processing. 

### Create a Python collection 

We need some data to start experimenting with RDDs. Let's create some sample data and see how RDDs handle it. To practice working with RDDs, we're going to use a simple Python list.

- Create a Python list `data` of integers between 1 and 1000 using the `range()` function. 
- Sanity check : confirm the length of the list (it should be 1000)


```python
nums = list(range(1,1001))
len(nums)
```




    1000



### Initialize an RDD

To initialize an RDD, first import `pyspark` and then create a SparkContext assigned to the variable `sc`. Use 'local[*]' as the master.


```python
import pyspark
sc = pyspark.SparkContext('local[*]')
```

Once you've created the SparkContext, you can use the `parallelize` method to create an rdd. Here, create one called `rdd` with 10 partitions using `data` as the collection you are parallelizing.


```python
rdd = sc.parallelize(nums,numSlices=10)
print(type(rdd))
```

    <class 'pyspark.rdd.RDD'>


Determine how many partitions are being used with this RDD with the getNumPartitions method.


```python
rdd.getNumPartitions()
```




    10



### Basic descriptive RDD actions

Let's perform some basic operations on our RDD. In the cell below, use the methods:
* `count`: returns the total count of items in the RDD 
* `first`: returns the first item in the RDD
* `take`: returns the first n items in the RDD
* `top`: returns the top n items
* `collect`: returns everything from your RDD


It's important to note that in a big data context, calling the collect method will often take a very long time to execute and should be handled with care!


```python
rdd.count()
```




    1000




```python
rdd.first()
```




    1




```python
rdd.take(10)
```




    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]




```python
rdd.top(10)
```




    [1000, 999, 998, 997, 996, 995, 994, 993, 992, 991]




```python
## Note: When you are dealing with big data, this could make your computer crash! It's best to avoid using the collect() method
rdd.collect()
```




    [1,
     2,
     3,
     4,
     5,
     6,
     7,
     8,
     9,
     10,
     11,
     12,
     13,
     14,
     15,
     16,
     17,
     18,
     19,
     20,
     21,
     22,
     23,
     24,
     25,
     26,
     27,
     28,
     29,
     30,
     31,
     32,
     33,
     34,
     35,
     36,
     37,
     38,
     39,
     40,
     41,
     42,
     43,
     44,
     45,
     46,
     47,
     48,
     49,
     50,
     51,
     52,
     53,
     54,
     55,
     56,
     57,
     58,
     59,
     60,
     61,
     62,
     63,
     64,
     65,
     66,
     67,
     68,
     69,
     70,
     71,
     72,
     73,
     74,
     75,
     76,
     77,
     78,
     79,
     80,
     81,
     82,
     83,
     84,
     85,
     86,
     87,
     88,
     89,
     90,
     91,
     92,
     93,
     94,
     95,
     96,
     97,
     98,
     99,
     100,
     101,
     102,
     103,
     104,
     105,
     106,
     107,
     108,
     109,
     110,
     111,
     112,
     113,
     114,
     115,
     116,
     117,
     118,
     119,
     120,
     121,
     122,
     123,
     124,
     125,
     126,
     127,
     128,
     129,
     130,
     131,
     132,
     133,
     134,
     135,
     136,
     137,
     138,
     139,
     140,
     141,
     142,
     143,
     144,
     145,
     146,
     147,
     148,
     149,
     150,
     151,
     152,
     153,
     154,
     155,
     156,
     157,
     158,
     159,
     160,
     161,
     162,
     163,
     164,
     165,
     166,
     167,
     168,
     169,
     170,
     171,
     172,
     173,
     174,
     175,
     176,
     177,
     178,
     179,
     180,
     181,
     182,
     183,
     184,
     185,
     186,
     187,
     188,
     189,
     190,
     191,
     192,
     193,
     194,
     195,
     196,
     197,
     198,
     199,
     200,
     201,
     202,
     203,
     204,
     205,
     206,
     207,
     208,
     209,
     210,
     211,
     212,
     213,
     214,
     215,
     216,
     217,
     218,
     219,
     220,
     221,
     222,
     223,
     224,
     225,
     226,
     227,
     228,
     229,
     230,
     231,
     232,
     233,
     234,
     235,
     236,
     237,
     238,
     239,
     240,
     241,
     242,
     243,
     244,
     245,
     246,
     247,
     248,
     249,
     250,
     251,
     252,
     253,
     254,
     255,
     256,
     257,
     258,
     259,
     260,
     261,
     262,
     263,
     264,
     265,
     266,
     267,
     268,
     269,
     270,
     271,
     272,
     273,
     274,
     275,
     276,
     277,
     278,
     279,
     280,
     281,
     282,
     283,
     284,
     285,
     286,
     287,
     288,
     289,
     290,
     291,
     292,
     293,
     294,
     295,
     296,
     297,
     298,
     299,
     300,
     301,
     302,
     303,
     304,
     305,
     306,
     307,
     308,
     309,
     310,
     311,
     312,
     313,
     314,
     315,
     316,
     317,
     318,
     319,
     320,
     321,
     322,
     323,
     324,
     325,
     326,
     327,
     328,
     329,
     330,
     331,
     332,
     333,
     334,
     335,
     336,
     337,
     338,
     339,
     340,
     341,
     342,
     343,
     344,
     345,
     346,
     347,
     348,
     349,
     350,
     351,
     352,
     353,
     354,
     355,
     356,
     357,
     358,
     359,
     360,
     361,
     362,
     363,
     364,
     365,
     366,
     367,
     368,
     369,
     370,
     371,
     372,
     373,
     374,
     375,
     376,
     377,
     378,
     379,
     380,
     381,
     382,
     383,
     384,
     385,
     386,
     387,
     388,
     389,
     390,
     391,
     392,
     393,
     394,
     395,
     396,
     397,
     398,
     399,
     400,
     401,
     402,
     403,
     404,
     405,
     406,
     407,
     408,
     409,
     410,
     411,
     412,
     413,
     414,
     415,
     416,
     417,
     418,
     419,
     420,
     421,
     422,
     423,
     424,
     425,
     426,
     427,
     428,
     429,
     430,
     431,
     432,
     433,
     434,
     435,
     436,
     437,
     438,
     439,
     440,
     441,
     442,
     443,
     444,
     445,
     446,
     447,
     448,
     449,
     450,
     451,
     452,
     453,
     454,
     455,
     456,
     457,
     458,
     459,
     460,
     461,
     462,
     463,
     464,
     465,
     466,
     467,
     468,
     469,
     470,
     471,
     472,
     473,
     474,
     475,
     476,
     477,
     478,
     479,
     480,
     481,
     482,
     483,
     484,
     485,
     486,
     487,
     488,
     489,
     490,
     491,
     492,
     493,
     494,
     495,
     496,
     497,
     498,
     499,
     500,
     501,
     502,
     503,
     504,
     505,
     506,
     507,
     508,
     509,
     510,
     511,
     512,
     513,
     514,
     515,
     516,
     517,
     518,
     519,
     520,
     521,
     522,
     523,
     524,
     525,
     526,
     527,
     528,
     529,
     530,
     531,
     532,
     533,
     534,
     535,
     536,
     537,
     538,
     539,
     540,
     541,
     542,
     543,
     544,
     545,
     546,
     547,
     548,
     549,
     550,
     551,
     552,
     553,
     554,
     555,
     556,
     557,
     558,
     559,
     560,
     561,
     562,
     563,
     564,
     565,
     566,
     567,
     568,
     569,
     570,
     571,
     572,
     573,
     574,
     575,
     576,
     577,
     578,
     579,
     580,
     581,
     582,
     583,
     584,
     585,
     586,
     587,
     588,
     589,
     590,
     591,
     592,
     593,
     594,
     595,
     596,
     597,
     598,
     599,
     600,
     601,
     602,
     603,
     604,
     605,
     606,
     607,
     608,
     609,
     610,
     611,
     612,
     613,
     614,
     615,
     616,
     617,
     618,
     619,
     620,
     621,
     622,
     623,
     624,
     625,
     626,
     627,
     628,
     629,
     630,
     631,
     632,
     633,
     634,
     635,
     636,
     637,
     638,
     639,
     640,
     641,
     642,
     643,
     644,
     645,
     646,
     647,
     648,
     649,
     650,
     651,
     652,
     653,
     654,
     655,
     656,
     657,
     658,
     659,
     660,
     661,
     662,
     663,
     664,
     665,
     666,
     667,
     668,
     669,
     670,
     671,
     672,
     673,
     674,
     675,
     676,
     677,
     678,
     679,
     680,
     681,
     682,
     683,
     684,
     685,
     686,
     687,
     688,
     689,
     690,
     691,
     692,
     693,
     694,
     695,
     696,
     697,
     698,
     699,
     700,
     701,
     702,
     703,
     704,
     705,
     706,
     707,
     708,
     709,
     710,
     711,
     712,
     713,
     714,
     715,
     716,
     717,
     718,
     719,
     720,
     721,
     722,
     723,
     724,
     725,
     726,
     727,
     728,
     729,
     730,
     731,
     732,
     733,
     734,
     735,
     736,
     737,
     738,
     739,
     740,
     741,
     742,
     743,
     744,
     745,
     746,
     747,
     748,
     749,
     750,
     751,
     752,
     753,
     754,
     755,
     756,
     757,
     758,
     759,
     760,
     761,
     762,
     763,
     764,
     765,
     766,
     767,
     768,
     769,
     770,
     771,
     772,
     773,
     774,
     775,
     776,
     777,
     778,
     779,
     780,
     781,
     782,
     783,
     784,
     785,
     786,
     787,
     788,
     789,
     790,
     791,
     792,
     793,
     794,
     795,
     796,
     797,
     798,
     799,
     800,
     801,
     802,
     803,
     804,
     805,
     806,
     807,
     808,
     809,
     810,
     811,
     812,
     813,
     814,
     815,
     816,
     817,
     818,
     819,
     820,
     821,
     822,
     823,
     824,
     825,
     826,
     827,
     828,
     829,
     830,
     831,
     832,
     833,
     834,
     835,
     836,
     837,
     838,
     839,
     840,
     841,
     842,
     843,
     844,
     845,
     846,
     847,
     848,
     849,
     850,
     851,
     852,
     853,
     854,
     855,
     856,
     857,
     858,
     859,
     860,
     861,
     862,
     863,
     864,
     865,
     866,
     867,
     868,
     869,
     870,
     871,
     872,
     873,
     874,
     875,
     876,
     877,
     878,
     879,
     880,
     881,
     882,
     883,
     884,
     885,
     886,
     887,
     888,
     889,
     890,
     891,
     892,
     893,
     894,
     895,
     896,
     897,
     898,
     899,
     900,
     901,
     902,
     903,
     904,
     905,
     906,
     907,
     908,
     909,
     910,
     911,
     912,
     913,
     914,
     915,
     916,
     917,
     918,
     919,
     920,
     921,
     922,
     923,
     924,
     925,
     926,
     927,
     928,
     929,
     930,
     931,
     932,
     933,
     934,
     935,
     936,
     937,
     938,
     939,
     940,
     941,
     942,
     943,
     944,
     945,
     946,
     947,
     948,
     949,
     950,
     951,
     952,
     953,
     954,
     955,
     956,
     957,
     958,
     959,
     960,
     961,
     962,
     963,
     964,
     965,
     966,
     967,
     968,
     969,
     970,
     971,
     972,
     973,
     974,
     975,
     976,
     977,
     978,
     979,
     980,
     981,
     982,
     983,
     984,
     985,
     986,
     987,
     988,
     989,
     990,
     991,
     992,
     993,
     994,
     995,
     996,
     997,
     998,
     999,
     1000]



## Map functions

Now that you've working a little bit with RDDs, let's make this a little more interesting. Imagine you're running a hot new ecommerce startup called BuyStuff, and you're trying to track of how much it charges customers from each item sold. In the next cell, we're going to create simulated data by multiplying the values 1-1000 be a random number from 0-1.


```python
import random
import numpy as np

nums = np.array(range(1,1001))
sales_figures = nums * np.random.rand(1000)
sales_figures
```




    array([7.46141789e-01, 1.05169804e+00, 2.01732715e+00, 1.75553921e-01,
           4.36245844e+00, 3.14786873e+00, 5.75731245e+00, 5.07518627e+00,
           3.90350827e+00, 5.73819730e+00, 7.19169912e+00, 1.14382446e+01,
           7.43240786e+00, 7.05807488e+00, 3.49761538e+00, 1.12645986e+01,
           3.19933209e+00, 1.66903439e+01, 1.17471483e+01, 5.18842800e+00,
           1.34427052e+00, 2.19053754e+01, 2.80857259e+00, 1.41908371e+01,
           4.17160350e+00, 1.94944491e+01, 2.12783547e+01, 2.34254012e+01,
           2.25104773e+01, 2.27939361e+01, 2.99156452e+01, 1.35040495e+01,
           1.65935287e+01, 2.62924596e+01, 3.24894271e+01, 2.40683798e+01,
           2.89984324e+01, 1.56111336e+01, 2.88337175e+01, 3.38635668e+01,
           3.71852144e+01, 2.38464980e+01, 8.68944529e+00, 2.24314509e+00,
           2.29557117e+01, 3.25632498e+01, 1.11844874e+01, 4.67535029e+01,
           7.50757004e+00, 1.88292939e+01, 1.62387339e+00, 7.56264801e+00,
           4.70588250e+01, 1.59365723e+01, 2.22874589e+00, 2.35468321e+01,
           9.26632141e+00, 1.55946827e+01, 3.86587176e+01, 5.06175436e+01,
           1.97052148e+01, 1.28826989e+01, 4.70940562e+00, 9.81808326e+00,
           1.60300258e+01, 1.84246659e+01, 4.96855903e+00, 6.74925210e+01,
           5.30733209e+01, 2.57742918e+00, 4.04492264e+01, 1.25488453e+01,
           9.01754644e+00, 7.04309612e+01, 1.67071356e+01, 4.55208272e+01,
           1.93347019e+01, 3.31246979e+01, 1.81589853e+01, 5.07630611e+01,
           7.70571443e+01, 2.77253471e+01, 4.08593469e+01, 4.61328297e+01,
           4.33152644e+01, 4.70949063e+01, 7.34639057e+01, 1.09857576e+01,
           4.30398609e+01, 5.64789066e+01, 3.15617124e+01, 6.59411436e+01,
           2.78140666e+01, 6.05760521e+01, 5.60171199e+01, 1.12826430e+01,
           6.89264620e+01, 8.31406659e+01, 4.40267690e+01, 4.58467544e+01,
           1.28620832e+01, 6.08649602e+01, 1.06914523e+01, 5.08360191e+01,
           6.43824918e+01, 8.11159628e+01, 5.97347292e+01, 2.90376889e+01,
           6.10326712e+01, 7.04890089e+01, 5.74974864e+01, 6.09222642e+01,
           1.78775581e+01, 2.23181115e+01, 7.92268831e+01, 3.48265464e+01,
           1.11332690e+02, 9.41972382e+01, 1.09749138e+01, 1.00878404e+02,
           3.65149486e+01, 7.00600788e+01, 1.32664971e+01, 4.31878469e+01,
           6.42586462e+01, 7.83687686e+01, 6.07269794e+01, 6.33852935e+01,
           2.13405319e+01, 2.17621118e+01, 6.80311955e+01, 1.18514407e+02,
           6.31295466e+01, 2.78378779e+00, 6.69711640e+01, 7.46087092e+01,
           1.24047384e+02, 1.15075888e+02, 1.32616214e+02, 5.35499992e+01,
           1.09313115e+01, 5.09474950e+01, 1.20565824e+02, 6.83772678e+01,
           1.34265988e+01, 1.03090930e+02, 1.07146846e+01, 8.31166647e+01,
           8.31378908e+01, 2.76943185e+01, 1.21787410e+02, 9.74940914e+01,
           9.63410270e+01, 5.47800265e+01, 1.44133528e+02, 2.94637882e+01,
           1.54788556e+02, 3.00061211e+01, 6.60188704e+01, 1.27801070e+02,
           6.20370047e+01, 3.53747005e+01, 1.49324040e+02, 1.04625721e+02,
           9.23092732e+01, 1.63042660e+01, 1.40461514e+02, 1.67057030e+02,
           1.50708825e+02, 1.16633348e+02, 1.45814798e+02, 7.41633856e+01,
           6.78136937e+01, 4.34071784e+01, 3.86415388e+01, 5.81598704e+01,
           1.42093330e+02, 1.06393850e+02, 7.75825124e+01, 6.18542133e+01,
           5.98810358e+00, 1.26977425e+02, 2.52799576e+01, 1.41937471e+02,
           7.30020467e+01, 3.64464745e+01, 1.19242021e+02, 4.44668809e+00,
           9.87224401e+01, 1.16676095e+02, 1.61911484e+02, 4.54280424e+01,
           4.29726457e+01, 1.46206171e+02, 1.05734137e+02, 6.03661182e+01,
           8.13702617e+01, 5.43182730e+01, 1.83787621e+02, 6.51938893e+01,
           5.02599433e+01, 5.90990454e+01, 4.96764860e+00, 4.39961984e+01,
           1.18845724e+02, 1.39484522e+02, 1.78081425e+02, 8.42929698e+01,
           7.97417445e+01, 1.75894626e+02, 1.77278937e+02, 1.72234821e+01,
           1.28265119e+02, 9.02639427e+01, 1.14870592e+01, 6.51556099e+01,
           1.50217493e+02, 1.40120622e+02, 1.35782473e+02, 4.02806011e+01,
           1.13670514e+02, 7.94468464e+01, 2.74998060e+01, 2.11995765e+02,
           1.10844735e+02, 3.71516923e+01, 1.36366755e+02, 5.12515608e+01,
           3.14159799e+01, 5.72198496e+01, 1.12405205e+02, 6.81733379e+01,
           1.09037097e+02, 1.20310621e+02, 1.55060380e+02, 6.69139934e+01,
           1.90220405e+02, 7.13950414e+01, 8.60701179e+01, 9.35722821e+01,
           2.08602587e+02, 7.54399062e+01, 2.00156831e+01, 7.26450358e+01,
           7.56677494e+01, 7.82403804e+01, 4.61669549e+01, 1.60891302e+02,
           1.84529575e+01, 1.98899379e+02, 2.42957693e+02, 5.69790333e+01,
           4.25433931e+00, 5.03640271e+01, 2.23676198e+02, 2.41633321e+02,
           3.22485692e+01, 1.39143166e+02, 1.19511085e+02, 1.09400540e+02,
           1.54303544e+01, 1.00731071e+02, 7.08413828e+01, 8.12405239e+01,
           1.00905469e+01, 3.67610151e+01, 2.53814780e+02, 4.17513330e+01,
           1.14894176e+02, 4.64319719e+01, 1.27220177e+02, 1.58388651e+02,
           5.12840343e+01, 2.23540716e+02, 1.65788279e+02, 9.43859726e+01,
           1.05083311e+02, 2.59729451e+02, 2.04793723e+02, 2.59032488e+02,
           1.76730313e+01, 8.55509750e+01, 1.72387813e+02, 2.46005408e+02,
           1.60520324e+02, 1.46735580e+02, 2.16855593e+02, 1.17359622e+02,
           2.40239455e+02, 6.01093805e+01, 2.49368807e+02, 2.05559149e+02,
           2.13153153e+01, 2.87304713e+02, 2.07359457e+02, 4.75538378e+01,
           2.16083926e+02, 1.10408142e+02, 2.80224197e+02, 9.77711655e+01,
           2.92778525e+02, 2.08349206e+02, 1.42208399e+01, 2.72033594e+02,
           3.03472747e+02, 3.02859045e+02, 1.37428463e+00, 1.69395156e+02,
           8.53580436e+01, 4.95130159e+01, 2.06913203e+02, 1.32727943e+02,
           2.60983335e+02, 1.10931137e+02, 4.50608437e+01, 1.58940739e+02,
           3.01202300e+01, 3.00206821e+02, 8.87953896e+01, 1.53901689e+02,
           2.44273342e+02, 2.69806864e+02, 2.81971323e+02, 1.87874694e+02,
           4.06522505e+01, 3.42970772e+01, 8.03786853e+01, 3.26187271e+02,
           9.26743075e+00, 4.67787313e+01, 1.25933294e+02, 1.35812701e+02,
           2.61027689e+02, 2.74898854e+02, 8.03623408e+01, 9.49953735e+01,
           1.25988064e+02, 3.60157597e+01, 3.00979178e+02, 2.88824204e+02,
           9.25579927e+01, 3.20250190e+02, 2.49237102e+02, 3.13640537e+02,
           7.81007032e+01, 8.98621305e+01, 2.26038739e+02, 2.74767888e+02,
           3.27923732e+02, 1.49845505e+02, 3.21564839e+02, 3.28374956e+01,
           1.83444056e+02, 2.51285290e+02, 1.49445031e+02, 3.03184133e+02,
           2.13187797e+02, 2.24113126e+02, 3.58995970e+02, 3.22376758e+00,
           2.42848674e+02, 2.91779245e+02, 3.30339551e+02, 5.10851549e+01,
           5.41820219e+01, 3.42908824e+02, 4.61027019e+01, 3.46096841e+02,
           1.23691453e+02, 1.66966878e+02, 3.31596806e+02, 1.63797617e+02,
           3.13767198e+02, 6.03579160e+01, 2.04125924e+02, 6.47838708e-01,
           2.05482514e+02, 2.72378728e+01, 1.33879806e+02, 1.92277010e+02,
           1.04932132e+02, 3.20515308e+02, 3.77905406e+02, 2.40365842e+02,
           9.13193310e+01, 2.91313216e+02, 8.97590767e+01, 2.81876586e+02,
           5.10183950e+01, 8.85234721e+00, 1.63697585e+02, 5.66825999e+01,
           2.70311098e+02, 3.73793334e+02, 2.56756518e+02, 6.50593506e+01,
           1.32670909e+02, 2.79200013e+02, 2.65847971e+02, 4.63992377e+01,
           6.50591980e+01, 2.80010309e+02, 2.53223879e+02, 4.03991920e+02,
           1.68507694e+02, 2.49089012e+02, 3.64206517e+01, 2.69611984e+02,
           5.99015879e+01, 1.88346177e+02, 2.19003819e+02, 1.28063820e+02,
           3.49538315e+02, 8.02813919e+01, 3.40318214e+02, 2.55321570e+02,
           3.04708040e+02, 2.64159635e+02, 1.70788151e+02, 3.24751282e+02,
           3.95081461e+02, 1.67512005e+02, 1.21606000e+02, 4.66694471e+01,
           1.73777021e+02, 2.73217945e+02, 3.57416989e+02, 2.31020428e+02,
           4.07201908e+02, 4.01201243e+02, 2.79773845e+02, 3.54985953e+02,
           2.69354330e+02, 8.51818395e+01, 3.36326992e+02, 1.36203029e+02,
           1.27506354e+01, 3.13452045e+02, 1.65472805e+02, 1.51248680e+02,
           5.40313394e+01, 1.08711301e+02, 4.09929176e+02, 6.87129191e+01,
           3.35606891e+02, 1.44343849e+02, 8.07469839e+01, 1.96301815e+02,
           2.05342956e+02, 3.62115931e+02, 1.45722862e+02, 2.53461319e+01,
           4.15649820e+01, 4.43368890e+02, 1.77859240e+02, 4.29656912e+02,
           3.57195674e+02, 4.04613409e+02, 1.97495251e+02, 8.21340969e+01,
           2.53248426e+02, 2.97787373e+02, 2.19728446e+02, 4.55496737e+02,
           3.55148187e+02, 1.40181828e+02, 2.97944611e+02, 4.57482075e+02,
           3.12121262e+02, 3.68067288e+02, 3.64934539e+02, 9.34390431e+01,
           4.25938406e+02, 4.35082984e+02, 2.79958917e+02, 4.38242868e+02,
           2.74758646e+02, 1.75771064e+02, 4.07777288e+02, 3.74900351e+02,
           4.43685298e+02, 1.80117774e+02, 2.29718057e+02, 3.17318770e+02,
           1.89779789e+02, 3.05471443e+01, 1.77424794e+02, 3.85033807e+02,
           1.13196545e+02, 4.25581032e+02, 2.99781193e+02, 1.74925075e+02,
           8.28477031e+01, 1.62373025e+02, 2.63464278e+02, 1.72995098e+02,
           3.28624176e+02, 6.27684751e+01, 1.91796689e+02, 3.92672698e+02,
           1.62526976e+02, 4.90828344e+02, 1.96338578e+01, 2.52596339e+02,
           1.28246604e+02, 1.50684313e+01, 2.41779748e+02, 4.33692614e+02,
           3.34625768e+02, 4.85134654e+02, 2.51448347e+01, 9.72030734e+01,
           3.86698923e+02, 2.53887966e+02, 4.53183781e+02, 9.98014668e+01,
           4.97652105e+02, 3.92395017e+02, 4.86701095e+02, 1.71721230e+02,
           1.36321137e+02, 5.69386934e+01, 2.10093571e+02, 5.06400017e+02,
           1.95388613e+02, 1.16462177e+02, 9.52143319e+01, 5.11526519e+02,
           2.06404521e+02, 8.80305824e+01, 1.31482428e+02, 2.91140888e+02,
           1.78766602e+01, 3.59324691e+02, 4.83005293e+02, 3.34667673e+02,
           5.35958882e+02, 4.41256665e+02, 1.58707361e+02, 2.06304610e+02,
           2.54688910e+02, 4.70905141e+01, 2.40349737e+02, 4.80777798e+02,
           3.32247538e+02, 2.95531564e+01, 9.72292432e+01, 1.20056143e+02,
           3.68774392e+02, 3.67813470e+01, 1.70874586e+02, 1.12673219e+02,
           1.67559271e+02, 1.07480968e+02, 3.04685119e+02, 4.57969182e+02,
           2.90034005e+02, 3.06670382e+02, 1.44845053e+02, 5.21819472e+02,
           1.81808990e+01, 3.12331063e+02, 1.93792934e+02, 4.01630617e+02,
           4.33023279e+01, 3.80396373e+02, 3.32245681e+02, 1.32426839e+02,
           3.68080054e+02, 1.00876551e+02, 3.75090289e+02, 3.81117702e+01,
           3.92191612e+02, 4.61736907e+02, 2.65384526e+02, 3.88656865e+02,
           5.03778081e+02, 1.40207797e+02, 3.26768918e+02, 3.38569250e+02,
           1.92946461e+02, 3.31284026e+02, 5.46542846e+02, 1.63633108e+02,
           1.72432816e+02, 2.37603334e+02, 2.63433570e+02, 5.69332557e+02,
           2.69694802e+02, 1.47190808e+02, 8.54201368e+01, 2.78702619e+01,
           3.51178323e+02, 3.04165895e+02, 1.08612998e+02, 5.74963756e+02,
           3.75232895e+02, 2.87934063e+02, 3.32012258e+02, 5.47650375e+02,
           2.45613084e+02, 2.23321477e+01, 1.82578096e+01, 5.41481152e+02,
           3.38026456e+02, 2.11973303e+02, 3.19200092e+02, 4.01691386e+02,
           2.20701243e+02, 3.54329192e+02, 5.89319083e+02, 3.22707886e+01,
           5.11147781e+02, 4.64970796e+02, 3.69523784e+02, 2.65714295e+02,
           5.84595891e+02, 1.94714434e+02, 9.37376485e+01, 1.48710279e+02,
           4.41645054e+02, 5.00581515e+02, 4.02798103e+02, 8.09162006e+01,
           2.85526659e+02, 3.88584106e+02, 3.04005680e+02, 1.54696272e+02,
           3.39196567e+02, 1.05229356e+02, 1.02034696e+02, 4.73327863e+02,
           3.83885440e+02, 3.94475686e+02, 3.53131099e+02, 8.40303294e+01,
           5.16783737e+02, 2.02328491e+02, 1.65967934e+02, 3.56209568e+02,
           5.47187455e+02, 3.80621707e+02, 2.40362600e+02, 1.98912934e+02,
           1.20135654e+02, 4.90949747e+02, 5.42037278e+02, 3.58413297e+02,
           3.81640147e+02, 2.09533207e+02, 1.35293271e+02, 5.98020746e+02,
           5.76239378e+02, 2.98106743e+02, 4.52003143e+02, 5.33845269e+02,
           4.81362794e+02, 2.67348321e+02, 5.44590355e+02, 3.79119233e+02,
           2.97652385e+02, 3.41279774e+02, 2.33342921e+02, 2.58183558e+02,
           5.88263751e+02, 1.63485816e+02, 7.17555836e+01, 3.66581415e+02,
           1.27059861e+02, 1.47719630e+01, 4.15496066e+01, 2.61585735e+02,
           4.06088318e+02, 1.55964941e+02, 5.01793037e+02, 8.04375648e+01,
           2.17405409e+02, 4.72063755e+02, 5.99912867e+01, 6.77883593e+02,
           2.96520669e+01, 4.59137420e+02, 6.65390646e+02, 1.25508387e+02,
           3.28572153e+02, 2.34118975e+02, 5.21222653e+02, 1.73093080e+02,
           1.25343081e+02, 5.90111034e+02, 4.25672884e+02, 7.53103502e+00,
           6.74930877e+01, 1.71205750e+01, 2.73048818e+02, 4.00394398e+02,
           2.47441056e+02, 1.28040582e+02, 1.94629762e+02, 2.20836135e+02,
           1.31771803e+02, 2.94605488e+02, 5.09470900e+02, 2.51911173e+02,
           2.72259079e+02, 4.62646244e+02, 5.90931203e+02, 3.67072212e+02,
           6.33704515e+02, 4.20738253e+01, 3.02738896e+01, 3.69501382e+02,
           2.53500845e+01, 5.88796783e+02, 4.89276058e+02, 5.93259887e+02,
           6.32464206e+02, 6.34856690e+02, 5.68535915e+02, 6.63586913e+02,
           2.90940266e+02, 2.46072484e+02, 6.79646809e+02, 2.00611624e+02,
           4.19186204e+02, 2.18942974e+02, 1.91832444e+02, 3.58572621e+02,
           2.54598119e+02, 6.88745106e+02, 8.23230887e+01, 6.96306400e+02,
           3.89082802e+02, 1.09784969e+02, 4.69645860e+02, 2.26652246e+02,
           5.18491073e+02, 3.49549289e+02, 2.94728042e+02, 5.96748101e+02,
           3.42687331e+02, 6.07313852e+02, 5.22557083e+02, 2.70139740e+02,
           3.06862787e+02, 6.52197920e+02, 4.02514928e+02, 1.09163979e+02,
           1.48820488e+02, 3.33378092e+02, 5.73074960e+02, 4.21464290e+02,
           2.40592546e+02, 1.36031227e+02, 4.61857558e+02, 4.29518951e+02,
           3.60508422e+02, 2.22742593e+02, 5.10980369e+02, 6.48825610e+02,
           2.58226975e+02, 6.66201280e+02, 6.65874794e+02, 9.76238556e+01,
           2.36248683e+02, 1.39177804e+00, 3.04679890e+02, 2.55702035e+02,
           7.42824360e+02, 2.45769413e+02, 4.76712229e+02, 9.30096356e+01,
           5.24333496e+02, 4.93017551e+02, 5.48553581e+02, 6.81749048e+01,
           6.41888331e+02, 7.73672969e+02, 4.98943226e+02, 1.01907201e+02,
           2.58116723e+00, 5.68545674e+02, 2.49505154e+02, 7.27138664e-01,
           3.61154709e+01, 3.83434676e+01, 7.51005166e+02, 7.47098845e+02,
           5.85230273e+02, 6.19057031e+02, 4.58028981e+02, 3.09875333e+02,
           1.91112167e+02, 2.66794370e+02, 6.05659041e+02, 1.55587109e+02,
           5.82411286e+02, 2.63529745e+02, 4.94996276e+02, 1.96309298e+02,
           2.47284759e+02, 7.98115333e+02, 6.40228096e+02, 5.43523369e+02,
           2.90654464e+02, 6.25363022e+02, 6.23977278e+01, 7.80211408e+02,
           4.57037619e+02, 4.93394977e+02, 7.50864246e+02, 5.95792278e+02,
           8.84176666e+01, 1.40960701e+02, 7.40446723e+01, 8.31732029e+01,
           8.05309128e+02, 1.84291862e+01, 4.51973274e+02, 6.56491643e+02,
           5.83568713e+02, 1.88670371e+02, 6.59168305e+02, 5.55051100e+02,
           1.64572100e+02, 2.78929828e+02, 3.30086378e+02, 1.73199854e+02,
           2.38107852e+02, 1.95254588e+02, 6.68388867e+02, 2.43309523e+02,
           5.85698597e+02, 4.39265078e+02, 1.33051539e+02, 8.08927452e+02,
           6.49963502e+02, 5.62931944e+02, 7.52339177e+02, 4.53052028e+02,
           4.21736279e+02, 5.32615561e+02, 3.01248630e+02, 7.69330353e+02,
           1.28766360e+02, 5.56286482e+02, 6.40315868e+02, 4.91519266e+02,
           4.26604762e+02, 2.79743081e+02, 3.35404502e+02, 5.04782005e+02,
           2.78791753e+02, 2.12726414e+02, 3.25053906e+02, 6.49230484e+02,
           5.55686550e+02, 7.93874814e+02, 2.44035398e+01, 6.71848995e+02,
           5.76422285e+02, 5.06355135e+02, 8.46795632e+02, 1.91729323e+02,
           1.80519056e+02, 5.84004632e+02, 2.98369658e+02, 2.65897368e+02,
           6.85937354e+02, 4.94846691e+02, 7.65808317e+02, 5.74030693e+02,
           2.05354250e+02, 7.29906858e+02, 2.96306403e+02, 8.02714131e+02,
           1.03350917e+02, 6.76535267e+02, 7.64373096e+01, 1.91914489e+02,
           5.37486174e+02, 8.80380664e+02, 1.65420403e+02, 2.70088208e+02,
           9.88050385e+01, 1.25927857e+02, 4.23878295e+02, 3.62379833e+02,
           2.87457680e+02, 6.71682561e+02, 5.53128328e+02, 5.61083196e+02,
           5.78139089e+02, 1.61830711e+02, 3.96978273e+02, 3.78767753e+02,
           1.91002750e+02, 3.64402436e+02, 6.68468327e+02, 5.07387453e+02,
           2.81876861e+02, 5.34727934e+02, 2.10451263e+02, 2.99089651e+02,
           7.06307283e+01, 7.11960888e+02, 7.14774178e+02, 8.14844101e+02,
           8.36881402e+02, 5.13151954e+02, 8.12945299e+01, 2.35832162e+02,
           2.44352700e+02, 3.22366089e+02, 4.35785934e+02, 3.67766059e+02,
           4.90936868e+02, 4.32661538e+02, 6.59128170e+02, 5.72179043e+02,
           7.01471530e+02, 9.21926422e+02, 5.45347021e+00, 5.56422374e+00,
           6.56179480e+02, 7.59688419e+02, 4.59113616e+02, 4.10544640e+02,
           9.97507058e+01, 3.15972266e+02, 1.51645029e+02, 4.14564560e+01,
           5.69618144e+02, 2.58184755e+02, 4.27019603e+01, 8.42108344e+02,
           4.24437341e+02, 6.19683540e+02, 9.13469798e+02, 4.67533111e+02,
           6.95960090e+02, 7.89662969e+02, 7.00277832e+01, 8.91366206e+02,
           3.08505498e+02, 8.00193098e+02, 4.20435615e+02, 8.52050259e+02,
           9.12538930e+02, 2.87357494e+02, 8.90799543e+02, 9.57057766e+01,
           2.30907664e+02, 2.75029661e+02, 2.95310194e+02, 2.50481155e+02,
           2.94246009e+02, 9.06965801e+02, 7.89045054e+02, 9.59733627e+01,
           4.92311940e+01, 1.67106955e+02, 4.27047369e+02, 2.74027748e+02,
           4.70519661e+02, 3.98984305e+02, 5.34195472e+02, 8.45321829e+02,
           7.46702456e+02, 7.20031004e+02, 6.34470930e+02, 8.51737812e+02,
           6.51308566e+02, 8.91424180e+02, 5.10722372e+02, 7.39432452e+02,
           6.11443854e+02, 7.90733353e+02, 2.87149447e+02, 5.81592052e+02,
           3.65435882e+02, 3.94069220e+02, 1.17611314e+02, 7.01712297e+02,
           9.36493969e+02, 9.32058763e+02, 1.25906397e+02, 4.43541945e+02,
           4.35485706e+02, 7.07344295e+02, 4.53620811e+02, 7.50590102e+02,
           9.71094416e+02, 6.40893575e+02, 7.94617453e+02, 8.70400364e+02,
           7.35530298e+02, 6.20895073e+02, 7.12889415e+02, 4.26464674e+02])



We now have sales prices for 1000 items currently for sale at BuyStuff. Now create an RDD called `price_items` using the newly created data with 10 slices. After you create it, use one of the basic actions to see what's in the RDD.


```python
price_items = sc.parallelize(sales_figures,numSlices=10)
price_items.take(4)
```




    [0.7461417892908068,
     1.0516980354349175,
     2.017327151511592,
     0.17555392107371448]



Now let's perform some operations on this simple dataset. To begin with, create a function that will take into account how much money BuyStuff will receive after sales tax has been applied (assume a sales tax of 8%). To make this happen, create a function called `sales_tax` that returns the amount of money our company will receive after the sale tax has been applied. The function will have this parameter:

* `item`: (float) number to be multiplied by the sales tax.


Apply that function to the rdd by using the map method and assign it to a variable `renenue_minus_tax`


```python
def sales_tax(num):
    return num * 0.92

revenue_minus_tax = price_items.map(sales_tax)
```

Remember, Spark has __lazy evaluation__, which means that the `sales_tax` function is a transformer that is not executed until you call use an action. Use one of the collection methods to execute the transformer now a part of the RDD and observe the contents of the `revenue_minus_tax` rdd.


```python
revenue_minus_tax.take(10)
```




    [0.6864504461475422,
     0.9675621926001241,
     1.8559409793906647,
     0.16150960738781733,
     4.013461762758611,
     2.8960392302571774,
     5.296727457231619,
     4.669171365867002,
     3.5912276052468246,
     5.279141514899373]



### Lambda Functions

Note that you can also use lambda functions if you want to quickly perform simple operations on data without creating a function. Let's assume that BuyStuff has also decided to offer a 10% discount on all of their items on the pre-tax amounts of each item. Use a lambda function within a map method to apply the additional 10% loss in revenue for BuyStuff and assign the transformed RDD to a new RDD called `discounted`.


```python
discounted = revenue_minus_tax.map(lambda x : x*0.9)
```


```python
discounted.take(10)
```




    [0.6178054015327881,
     0.8708059733401117,
     1.6703468814515983,
     0.1453586466490356,
     3.61211558648275,
     2.6064353072314597,
     4.767054711508457,
     4.202254229280302,
     3.232104844722142,
     4.751227363409436]



## Chaining Methods

You are also able to chain methods together with Spark. In one line, remove the tax and discount from the revenue of BuyStuff use a collection method to see the 15 costliest items.


```python
price_items.map(sales_tax).map(lambda x : x*0.9).top(15)
```




    [804.0661762797445,
     775.4170061851304,
     771.7446559805785,
     763.3550777631568,
     756.3529929007062,
     755.5822341550767,
     750.9676834306276,
     738.0992206991075,
     738.0512183644807,
     737.5820216948556,
     728.9551898162994,
     720.6915017879135,
     705.4976143552012,
     705.2389084005003,
     701.1467832856864]



## RDD Lineage


We are able to see the full lineage of all the operations that have been performed on an RDD by using the `RDD.toDebugString()` method. As your transformations become more complex, you are encouraged to call this method to get a better understanding of the dependencies between RDDs. Try calling it on the `discounted` RDD to see what RDDs it is dependent on.


```python
discounted.toDebugString()
```




    b'(10) PythonRDD[31] at RDD at PythonRDD.scala:49 []\n |   ParallelCollectionRDD[23] at parallelize at PythonRDD.scala:184 []'



### Map vs. Flatmap

Depending on how you want your data to be outputted, you might want to use flatMap rather than a simple map. Let's take a look at how it performs operations versus the standard map. Let's say we wanted to maintain the original amount BuyStuff receives for each item as well as the new amount after the tax and discount are applied. Create a map function that will a tuple with (original price, post-discount price).


```python
mapped = price_items.map(lambda x: (x, x*0.92 *0.9))
print(mapped.count())
print(mapped.take(10))
```

    1000
    [(0.7461417892908068, 0.6178054015327881), (1.0516980354349175, 0.8708059733401117), (2.017327151511592, 1.6703468814515983), (0.17555392107371448, 0.1453586466490356), (4.3624584377810995, 3.61211558648275), (3.14786872854041, 2.6064353072314597), (5.75731245351263, 4.767054711508457), (5.075186267246741, 4.202254229280302), (3.9035082665726355, 3.232104844722142), (5.738197298803666, 4.751227363409436)]


Note that we have 1000 tuples created to our specification. Let's take a look at how flatMap differs in its implementation. Use the `flatMap` method with the same function you created above.


```python
flat_mapped = price_items.flatMap(lambda x : (x, x*0.92*0.9 ))
print(flat_mapped.count())
print(flat_mapped.take(10))
```

    2000
    [0.7461417892908068, 0.6178054015327881, 1.0516980354349175, 0.8708059733401117, 2.017327151511592, 1.6703468814515983, 0.17555392107371448, 0.1453586466490356, 4.3624584377810995, 3.61211558648275]


Rather than being represented by tuples, all of the  values are now on the same level. When we are trying to combine different items together, it is sometimes necessary to use flatmap rather than map in order to properly reduce to our specifications. This is not one of those instances, but int he upcoming lab, you just might have to use it.

## Filter
After meeting with some external consultants, BuyStuff has determined that its business will be more profitable if it focuses on higher ticket items. Now, use the filter method to select items that bring in more than $300 after tax and discount have been removed. A filter method is a specialized form of a map function that only returns the items that match a certain criteria. In the cell below:
* use a lambda function within a filter function to meet the consultant's suggestion's specifications. set RDD = `selected_items`
* calculate the total number of items remaining in BuyStuff's inventory


```python
selected_items = discounted.filter(lambda x: x>300)
selected_items.count()
```




    269



## Reduce

Now it's time to figure out how much money BuyStuff would make from selling one of all of it's items after they've reduced their inventory. Use a reduce method with a lambda function to to add up all of the values in the RDD. Your lambda function should have two variables.


```python
selected_items.reduce(lambda x,y :x + y)
```




    124995.25228376321



The time has come for BuyStuff to open up shop and start selling it's goods. It only has one of each item, but it's allowing 50 lucky users to buy as many items as they want while they remain in stock. Within seconds, BuyStuff is sold out. Below, you'll find the sales data in an RDD with tuples of (user, item bought).


```python
import random
random.seed(42)
# generating simulated users that have bought each item
sales_data = selected_items.map(lambda x: (random.randint(1,50),x))

sales_data.take(7)
```




    [(33, 312.9056760263304),
     (20, 309.5008808485666),
     (50, 334.5053096842322),
     (3, 327.1274495608138),
     (33, 337.16317984925684),
     (20, 332.19462884216404),
     (37, 339.42135737718223)]



It's time to determine some basic statistics about BuyStuff users.

Let's start off by creating an RDD that determines how much each user spent in total.
To do this we can use a method called __reduceByKey__ to perform reducing operations while grouping by keys. After you have calculated the total, use the __sortBy__ method on the RDD to rank the users from highest spending to least spending.




```python
total_spent = sales_data.reduceByKey(lambda x,y :x + y)
total_spent.take(10)
```




    [(50, 2777.696959541504),
     (20, 4214.479047385745),
     (40, 5449.469172051653),
     (30, 2158.938078822225),
     (10, 4391.378895688973),
     (11, 3113.512422503823),
     (31, 5537.261581317268),
     (21, 1619.9124917965592),
     (1, 1989.3379801015024),
     (41, 987.6845080551243)]




```python
total_spent.sortBy(lambda x: x[1],ascending = False).collect()
```




    [(33, 8502.258699603124),
     (13, 8415.297489176775),
     (31, 5537.261581317268),
     (40, 5449.469172051653),
     (38, 4913.359912052394),
     (8, 4759.532719971837),
     (6, 4499.402344033714),
     (10, 4391.378895688973),
     (9, 4248.698412608107),
     (14, 4240.695095289299),
     (20, 4214.479047385745),
     (17, 4189.827591560459),
     (3, 3979.760150509989),
     (16, 3315.2678985587045),
     (29, 3167.526253511516),
     (27, 3128.799407596127),
     (11, 3113.512422503823),
     (22, 2982.062235528268),
     (46, 2923.9753596726296),
     (36, 2891.882573086974),
     (12, 2855.365305006478),
     (50, 2777.696959541504),
     (35, 2402.2629845885003),
     (48, 2378.743466938473),
     (15, 2287.2707284612197),
     (5, 2246.3162155189384),
     (30, 2158.938078822225),
     (49, 2122.302175933297),
     (1, 1989.3379801015024),
     (44, 1843.512625796834),
     (37, 1650.8160083774778),
     (21, 1619.9124917965592),
     (32, 1588.5481526564026),
     (47, 1395.6985306618362),
     (42, 1218.850852684454),
     (23, 1063.3081859076956),
     (41, 987.6845080551243),
     (34, 804.0661762797445),
     (28, 783.283436397277),
     (43, 780.9366466972821),
     (4, 590.2724352921812),
     (39, 585.6810765408092)]



Next, let's determine how many items were bought per user. This can be solved in one line using an RDD method. After you've counted the total number of items bought per person, sort the users from most number of items bought to least number of items. Time to start a customer loyalty program!


```python
total_items = sales_data.countByKey()
sorted(total_items.items(),key=lambda kv:kv[1],reverse=True)
```




    [(10, 16),
     (38, 13),
     (49, 13),
     (13, 12),
     (14, 12),
     (17, 10),
     (37, 10),
     (41, 9),
     (15, 9),
     (5, 8),
     (46, 8),
     (23, 7),
     (7, 7),
     (1, 6),
     (4, 6),
     (43, 6),
     (8, 6),
     (3, 6),
     (11, 6),
     (33, 5),
     (44, 5),
     (27, 5),
     (12, 5),
     (39, 5),
     (24, 5),
     (2, 5),
     (29, 5),
     (9, 4),
     (48, 4),
     (40, 4),
     (6, 4),
     (42, 4),
     (16, 4),
     (21, 4),
     (22, 3),
     (30, 3),
     (20, 3),
     (18, 3),
     (26, 3),
     (31, 3),
     (35, 2),
     (36, 2),
     (50, 2),
     (28, 2),
     (34, 2),
     (19, 2),
     (47, 1)]



### Additional Reading

- [The original paper on RDDs](https://cs.stanford.edu/~matei/papers/2012/nsdi_spark.pdf)
- [RDDs in Apache Spark](https://data-flair.training/blogs/create-rdds-in-apache-spark/)
- [Programming with RDDs](https://runawayhorse001.github.io/LearningApacheSpark/rdd.html)
- [RDD Transformations and Actions Summary](https://www.analyticsvidhya.com/blog/2016/10/using-pyspark-to-perform-transformations-and-actions-on-rdd/)

## Summary

In this lab we went through a brief introduction to RDD creation from a Python collection, setting a number of logical partitions for an RDD, and extracting lineage and of an RDD in a spark application. We also used transformations and actions to perform calculations across RDDs on a distributed setup. Up next, you'll get the chance to apply these transformations on different books to calculate word counts and various statistics.


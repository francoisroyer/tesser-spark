# Spark backend for Tesser

This module uses Flambo to execute Tesser folds (a kind of *associative* and *commutative* monoids) on Spark. You can in particular run code like this in a Spark context:

```clj
(defn calc [] (->> (t/map inc)      ; Increment each number
                   (t/filter odd?)  ; Take only odd numbers
                   (t/fold +)
                   ))

(spark/fold sc 
        (range 10)
        nil ;collect if nil
        #'calc)
=> 25
```

Anything that can be expressed as a Tesser fold on a collection of records (distributed as  RDD) can be run on Spark, and get
better scaling and resource management at scale. You could for example design your analytics pipeline locally or on a single server for easier debugging and checking, and then scale up on a large Spark cluster. Or design a lot of small metrics and separate reports and fuse them into a single distributed job.

We'll explain in the remainder of this document how this works. 
We leverage the fact that Tesser exposes compiled folds as a map
of six functions that can each be implemented on the desired backend.

```clj
{:reducer-identity  (fn [] ...)
 :reducer           (fn [accumulator input] ...)
 :post-reducer      (fn [accumulator] ...)
 :combiner-identity (fn [] ...)
 :combiner          (fn [accumulator post-reducer-result])
 :post-combiner     (fn [accumulator] ...)}
```

An RDD (Resilient Distributed Dataset in Spark's parlance) is actually a collection of collections, which fits nicely into
Tesser's *chunk* abstraction. Given a Spark context (exposed to you via Flambo's API), we can define a simple RDD like so:

```clj
(def c (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "tesser")))

(def sc (f/spark-context c))
(def data (f/parallelize sc [0 1 2 3 4 5 6 7 8 9]))
```

Now let's define what happens in Spark when a fold needs to be executed (note: this could be expressed in a similar way using Sparkling's API).

```clj
(defn fold-reducer [input-rdd fold-var fold-args]
					(-> input-rdd
						(f/repartition 2)
						(f/map-partitions-with-index 
							(f/fn [i xs] 
               ;Deref and compile fold here, extract functions
               (let [f (-> fold-var
                           (deref)
                           (apply fold-args)
                           (t/compile-fold))
                     red-id (:reducer-identity f) 
                     red-func (:reducer f) 
                     red-post (:post-reducer f)]
								(.iterator 
									(vector (vector 0 ;This is a key used by the combiner
										(red-post (reduce red-func (red-id) (iterator-seq xs )) )
										))))))  ;reduce data here
						(f/map-to-pair (f/fn [[i x]] (ft/tuple i x) ))
						)))
```

We first call .partitionWithIndex on the input RDD, apply the reducer-identity, reducer and post-reducer functions in the correct order, and then return an iterator for the reduced collection chunk.

The next piece of code implements the combiner step:

```clj
(defn fold-combiner [reduced-rdd fold-var fold-args]
  (let [f (-> fold-var
              (deref)
              (apply fold-args)
              (t/compile-fold))
        comb-id (:combiner-identity f) 
        comb-func (:combiner f)]
	(-> reduced-rdd
		(f/combine-by-key
			(fn [row] (comb-func (comb-id) row) ) ;call id then f on row
			(fn [acc row] (comb-func acc row) )
			(fn [acc1 acc2] (comb-func acc1 acc2) )
			)
    ;Remove index from post-reducer output
		(f/map f/untuple)
		(f/map last)
		)))
```

The combiner takes the reduced RDD and the original fold, and calls Spark's .combineByKey method with the combiner function
applied to each member, after seeding the accumulator with the combiner-identity function.

All that is left to do is tie up the reducer and combiner functions into a single fold function, followed by a call to 
post-combiner and a collect action:

```clj
(defn fold
	"Fold executed in a Spark context"
	[sc input workdir fold-var & fold-args]
	(let [f (-> fold-var
              (deref)
              (apply fold-args)
              (t/compile-fold))
        comb-post (:post-combiner f)]
		(-> sc (f/parallelize input) 
      (fold-mapper fold-var fold-args)
      (fold-combiner fold-var fold-args)
			(f/collect)
      first
      (comb-post)
			)))
```

The high level features of Tesser still work, in particular the composability and convenience offered by the fuse function:

```clj
(require '[tesser.core :as t])

(def records [{:year         1986
               :lines-of-code {"ruby" 100,
                                "c"    1693}}
              {:year         2004
               :lines-of-code {"ruby" 100,
                                "c"    1693}}
              ])

(defn report [] (t/fuse {:year-range (t/range (t/map :year))
                         :total-code (->> (t/map :lines-of-code)
                                          (t/facet)
                                          (t/reduce + 0))}))

(fold sc ;conf
        records
        nil ;collect if nil
        #'report)
=> {:year-range [1986 2004], :total-code {"ruby" 200, "c" 3386}}
```

Enjoy Tesser on Spark!

# Known to work:
- map
- filter
- mapcat
- range
- frequencies
- fold

# Not implemented:
- take

# TODO:
- implement take fn
- use midje to test flambo backend
- test math
- prepare for pull request
- Performance benchmark
- Find out the best strategy to scale the combiner step


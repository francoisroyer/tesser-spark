(ns tesser.spark
	(:require [flambo.conf :as conf]
		[flambo.api :as f]
		[flambo.tuple :refer [tuple]]
		[tesser
		[utils :refer :all]
		[core :as t]]
		))

(require '[flambo.conf :as conf])
(require '[flambo.api :as f])
(require '[flambo.tuple :as ft :refer [tuple]])
(require '[tesser [utils :refer :all] [core :as t]])


;================================================================================
; Lifted from Hadoop namespace
;================================================================================

(defn resolve+
  "Resolves a symbol to a var, requiring the namespace if necessary. If the
  namespace doesn't exist, throws just like `clojure.core/require`. If the
  symbol doesn't exist after requiring, returns nil."
  [sym]
  (or (resolve sym)
      (let [ns (->> sym str (re-find #"(.+)\/") second symbol)]
        (require ns)
        (resolve sym))))

(defn rehydrate-fold
  "Takes the name of a function that generates a fold (a symbol) and args for
  that function, and invokes the function with args to build a fold, which is
  then compiled and returned."
  [fold-name fold-args]
  (-> fold-name
      resolve+
      deref
      (apply fold-args)
      t/compile-fold))


;================================================================================
; Fold mapper and reducer
;================================================================================

'(defn fold-mapper
  "A generic, stateful Spark mapper for applying a fold to a RDD.
  This function returns a mapper for fold defined by make-fold
  applied to fold-name & additional args."
  {::mr/source-as :vals
   ::mr/sink-as   :vals}
  [fold-name fold-args input]
  (list (try (let [fold (rehydrate-fold fold-name fold-args)
                   red  (:reducer fold)
                   post (:post-reducer fold)]
               (post
                 (reduce (fn [acc line]
                           (try
                             (red acc line)
                             (catch Exception e
                               (reduced (serialize-error acc line e)))))
                         ((:reducer-identity fold))
                         input)))
             (catch Exception e
               (serialize-error nil nil e)))))

'(defn fold-reducer
  "This function returns a Spark reducer for fold defined by make-fold
  applied to fold-name & additional args"
  {::mr/source-as :vals
   ::mr/sink-as   :vals}
  [fold-name fold-args input]
  (list (try (let [fold (rehydrate-fold fold-name fold-args)
                   combiner (:combiner fold)
                   combined (reduce (fn [acc x]
                                      (try
                                        (if (error? x)
                                          (reduced x)
                                          (combiner acc x))
                                        (catch Exception e
                                          (reduced (serialize-error acc x e)))))
                                    ((:combiner-identity fold))
                                    input)]
               (if (error? combined)
                 combined
                 ((:post-combiner fold) combined)))
             (catch Exception e
               (serialize-error nil nil e)))))


(defn fold*
  "Takes a Parkour graph and applies a fold to it. Takes a var for a function,
  taking `args`, which constructs a fold. Returns a new (unexecuted) graph.
  The output of this job will be a single-element Fressian structure containing
  the results of the fold applied to the job's inputs."
  [graph fold-var & args]
  (let [fold-name (var->sym fold-var)]
    (-> graph
        (pg/map #'fold-mapper fold-name args)
        (pg/partition [NullWritable FressianWritable])
        (pg/reduce #'fold-reducer fold-name args))))


(defn execute
  "Like `parkour.graph/execute`, but specialized for folds. Takes a parkour
  graph, a jobconf, and a job name. Executes the job, then returns a sequence
  of fold results. Job names will be automatically generated if not provided."
  ([graph conf]
   (execute graph conf (gen-job-name!)))
  ([graph conf job-name]
   ; For each phase, extract the first tuple, then the value.
   (map (comp second reduce-first)
        (pg/execute graph conf job-name))))



(defn fold
  "A simple, all-in-one fold operation. Takes a jobconf, workdir, input dseq,
  var which points to a fold function, and arguments for the fold function.
  Runs the fold against the dseq and returns its results. Names output dsink
  after metadata key :tesser.hadoop/output-path in fold symbol. If absent, uses
  the conf key tesser.hadoop.output-path and finally falls back
  to the fold symbol. On error, throws an `ex-info`."
  [conf input workdir fold-var & args]
  (let [in       (pg/input input)
        path     (output-path conf fold-var)]
    (try
      (let [x (-> (apply fold* in fold-var args)
                  (pg/output (dsink workdir path))
                  (execute conf)
                  first)]
        (when (error? x) (throw (ex-info "Hadoop fold error" x)))
        x))))



'(def conf (-> (conf/spark-conf)
           (conf/master "local[8]") ; 8 partitions
           (conf/app-name "flambo_test")))

'(def sc (f/spark-context conf))

'(defn flambo-word-count [filename]
    (-> (f/text-file sc filename)
        (f/flat-map (f/fn [line] (split-line line)))
        (f/map (f/fn [word] [word 1]))
        (f/count-by-key)
        (->> (write-result "output-flambo.csv"))))




;================================================================================
; Basic Flambo test
;================================================================================

(def c (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "tesser")))

(def sc (f/spark-context c))
(def data (f/parallelize sc [0 1 2 3 4 5 6 7 8 9]))

(defn fold-mapper [input-rdd id f post] ;[fold-name fold-args input]
				(let [x0 (id)
					;fold (rehydrate-fold fold-name fold-args)
					;red (:reducer fold)
					;post (:post-reducer fold)
				    ] ;call :reducer-identity here
					(-> input-rdd
						(f/repartition 2)
						(f/map-partitions-with-index 
							(f/fn [i xs] 
								(.iterator 
									(vector (vector 0 ;TODO insert key==0 here - or modulo partition?
										(post (reduce f x0 (iterator-seq xs )) )
										)))))  ;reduce data here
						;TODO map on key/values to turn into a rdd of tuples (1,value)
						(f/map-to-pair (f/fn [[i x]] (ft/tuple i x) ))
						;(f/collect)
						)))

;(def output (fold-mapper data (constantly 0) + inc))

;TODO remove index from post-reducer output
;TODO make sure output of post-reducer has same shape as combiner-identity
;TODO mergeValue and mergeCombiner should be identical

;(def output (f/parallelize-pairs sc [ (tuple 0 25) (tuple 1 26) ]))
;(def output (f/parallelize sc [ [0 25] [1 26] ]))

(defn fold-combiner [reduced-rdd id f post] 
	(-> reduced-rdd
		(f/combine-by-key
			(fn [row] (f (id) row) ) ;call id then f on row
			(fn [acc row] (f acc row) )
			(fn [acc1 acc2] (f acc1 acc2) )
			)
		(f/map f/untuple)
		(f/map last)
		(f/map post)
		))

(-> data
	(fold-mapper (constantly 0) + inc)
	(fold-combiner (constantly 0) + inc)
	(f/collect)
  vec)
;=> (48)

;TODO should change all keys to a single one? Multiple stages of combiner??


(defn fold
	"Fold on Spark. One Spark context is created per fold."
	[sc input workdir fold-var & fold-args]
	(let [;sc (f/spark-context conf)
		f (-> fold-var
			(deref)
			(apply fold-args)
			(t/compile-fold))]
		(-> sc (f/parallelize input) ;if string -> resolve scheme, if seq -> parallelize
			(fold-mapper (:reducer-identity f) (:reducer f) (:post-reducer f))
			(fold-combiner (:combiner-identity f) (:combiner f) (:post-combiner f))
			;write to workdir or collect data if workdir is nil
			(f/collect)
			)))


;TODO pass conf or Spark context??

(t/tesser ;[[1] [2 3]]
          [[1 2 3]]
          (t/fold {:reducer-identity  (constantly 0)
                   :reducer           +
                   :post-reducer      identity
                   :combiner-identity (constantly 0)
                   :combiner          +
                   :post-combiner     identity}))
;=> 6

(defn analyze [] 
	(t/fold {:reducer-identity  (constantly 0)
		:reducer           +
		:post-reducer      identity
		:combiner-identity (constantly 0)
		:combiner          +
		:post-combiner     identity}))

(fold sc ;conf
        ;(text/dseq "hdfs:/some/file/part-*")
        [[1] [2 3]]
        ;[0 1 2 3 4 5 6 7 8 9]
        ;"hdfs:/tmp/tesser"
        nil ;collect if nil
        #'analyze)
;=> 45










;{:reducer-identity  (fn [] ...)
; :reducer           (fn [accumulator input] ...)
; :post-reducer      (fn [accumulator] ...)
; :combiner-identity (fn [] ...)
; :combiner          (fn [accumulator post-reducer-result])
; :post-combiner     (fn [accumulator] ...)}

 
;; Show map then
(def mapped (-> data
    (f/map (f/fn [x] (* x 10)))
    (f/collect)))
mapped
(println mapped)
 
;; Filtering
(def filtered (-> data
    (f/filter (f/fn [x] (= x 1)))
    (f/collect)))
filtered
(println filtered)
 
;; Square sum
;; (def sq (-> (f/parallelize sc [1 2 3 4])
;;             (f/map f/square)
;;             (f/reduce +)
;;             (f/collect)
;;             ))
;; sqk
 
;;;;;;;;;;;;;;;;

'(def c (-> (conf/spark-conf)
           (conf/master "local[8]") ; 8 partitions
           (conf/app-name "flambo_test")))

'(def sc (f/spark-context c))

'(defn flambo-word-count [filename]
    (-> (f/text-file sc filename)
        (f/flat-map (f/fn [line] (split-line line)))
        (f/map (f/fn [word] [word 1]))
        (f/count-by-key)
        (->> (write-result "output-flambo.csv"))))




(ns correlation-mock.core
  (:gen-class)
  (require [milena.admin       :as admin]
           [milena.produce     :as produce]
           [milena.consume     :as consume]
           [milena.serialize   :as serialize]
           [milena.deserialize :as deserialize]
           [clojure.core.async :as async]
           [clojure.data.json :as json]))

(def sam-manual-cmds-topic "sam.manual_cmds")
(def correlation-command-topic "correlation.manual_command_outcomes")

(defn init-kafka [nodes group-id]
  (let [C (consume/make {:nodes              nodes
                         :deserializer-key   deserialize/string
                         :deserializer-value deserialize/string
                         :config             {:group.id           group-id
                                              :enable.auto.commit false
                                              :auto.offset.reset "earliest"}})
        P (produce/make {:nodes            nodes
                         :serializer-key   serialize/string
                         :serializer-value serialize/string })]
    {:consumer C :producer P}))

(defn correlate-m [m]
  (assoc 
   (-> (:value m)
       (json/read-str :key-fn keyword))
   :successful true))

(defn produce-message [message P topic partition]
  (produce/commit P
                  {:topic      topic
                   :partition partition
                   :key       (or (get-in message [:value :request_id])
                                  "default-key")
                   :value     message}
                  (fn callback [exception meta]
                    (println message :okay? (boolean exception)))))

(defn message-handler [m C-P-map]
  "Handle Kafka messages"
  (let [messages (->> m (map correlate-m))]
    (doall (map #(produce-message (json/write-str %)
                                  (:producer C-P-map)
                                  correlation-command-topic
                                  nil) messages))))

(defn consumer-listen [C-P-map topic n-loops]
  (println (format "Subscribing to topic %s" topic))
  (consume/listen (:consumer C-P-map) [[topic 0]])
  
  (println "Starting pool loop")
  (async/go-loop []
    (let [consumed-messages (consume/poll (:consumer C-P-map) 1000)]
      (message-handler consumed-messages C-P-map)
      (consume/commit-async (:consumer C-P-map)
                            (fn commit-print [execption offsets]
                              (if execption
                                (println (format "Coommit error:\n%s" execption))))))
    (recur)))

(defn -main
  "Correlation mock start"
  [& args]
  (let [ C-P-map (init-kafka [["localhost" 9092]] "bob-1")]
    (consumer-listen C-P-map sam-manual-cmds-topic 200)
    (loop [] (recur))))

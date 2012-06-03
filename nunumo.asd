;;;; info.read-eval-print.nando.asd

(asdf:defsystem :nunumo
  :serial t
  :components ((:file "package")
               (:file "type")
               (:file "util")
               (:file "serialize")
               (:file "thread")
               (:file "thread-pool")
               (:file "mmap")
               (:file "heap")
               (:file "server")
               (:file "client")
               (:file "nunumo")
               (:file "skip-list")
               (:file "skip-list-nunumo")
               (:file "in-memory-skip-list")
               (:file "in-memory-skip-list-nunumo")
               (:file "nunumo-server")
               (:file "nunumo-client"))
  :depends-on (:bordeaux-threads
               :anaphora
               :hu.dwim.defclass-star
               :sb-concurrency
               :usocket
               :md5
               :flexi-streams
               :info.read-eval-print.series-ext))


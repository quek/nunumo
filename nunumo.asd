;;;; info.read-eval-print.nando.asd

(asdf:defsystem :nunumo
  :serial t
  :components ((:file "package")
               (:file "type")
               (:file "util")
               (:file "mmap")
               (:file "heap")
               (:file "byte-heap-file")
               (:file "thread")
               (:file "thread-pool")
               (:file "server")
               (:file "client")
               (:file "nunumo")
               (:file "nunumo-server")
               (:file "nunumo-client")
               (:file "skip-list")
               (:file "in-memory-skip-list-nunumo"))
  :depends-on (:bordeaux-threads
               :anaphora
               :hu.dwim.defclass-star
               :sb-concurrency
               :usocket
               :info.read-eval-print.series-ext))


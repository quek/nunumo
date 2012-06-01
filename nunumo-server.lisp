(in-package :nunumo)

(defclass* nunumo-server (server in-memory-skip-list-nunumo)
  ())

(defmethod nunumo-open ((nunumo nunumo-server) &key (default-nunumo t))
  (server-start nunumo
                (lambda (stream)
                  (with-standard-io-syntax
                    (loop for sexp = (print (read stream nil))
                          while sexp
                          for result = (print (eval `(let ((*nunumo* ,nunumo))
                                                       ,sexp)))
                          do (print result stream)
                             (force-output stream)))))
  (call-next-method nunumo :default-nunumo default-nunumo))

(defmethod nunumo-close ((nunumo nunumo-server))
  (server-stop nunumo)
  (call-next-method))

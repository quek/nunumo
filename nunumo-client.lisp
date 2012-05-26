(in-package :nunumo)

(defclass* nunumo-client (client nunumo)
  ())

(defmethod nunumo-open ((nunumo nunumo-client) &key (default-nunumo t))
  (client-open nunumo)
  (call-next-method nunumo :default-nunumo default-nunumo))

(defmethod nunumo-close ((nunumo nunumo-client))
  (client-close nunumo)
  nunumo)

(defmethod %get ((nunumo nunumo-client) key)
  (client-write nunumo `(get ',key))
  (client-flush nunumo)
  (client-read nunumo))

(defmethod %set ((nunumo nunumo-client) key value)
  (client-write nunumo `(set ',key ',value))
  (client-flush nunumo)
  (client-read nunumo))



(defun test ()
  (let ((server (make-instance 'nunumo-server))
        (client (make-instance 'nunumo-client)))
    (nunumo-open server)
    (nunumo-open client)
    (unwind-protect
         (progn
           (set 'foo "bar")
           (print "done set.")
           (get 'foo))
      (nunumo-close client)
      (nunumo-close server))))

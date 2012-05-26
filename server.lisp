(in-package :nunumo)

(defparameter *port* 1958)

(defgeneric server-start (server function))
(defgeneric server-stop (server))


(defclass* server ()
  ((host usocket:*wildcard-host*)
   (port *port*)
   (listen-thread nil)
   (number-of-thread 4)))

(defmethod server-start ((server server) heandler)
  (setf (listen-thread-of server)
        (sb-thread:make-thread
         (lambda ()
           (usocket:with-socket-listener (socket (host-of server) (port-of server) :reuse-address t)
             (with-thread-pool (thread-pool (number-of-thread-of server))
               (loop
                 (add-job thread-pool
                          (let ((client-socket (usocket:socket-accept socket)))
                            (lambda ()
                              (unwind-protect
                                   (let ((client-stream (usocket:socket-stream client-socket)))
                                     (funcall heandler client-stream)
                                     (force-output client-stream))
                                (usocket:socket-close client-socket)))))))))
         :name "nunumo:server")))

(defmethod server-stop ((server server))
  (sb-thread:terminate-thread (listen-thread-of server))
  (setf (listen-thread-of server) nil))



#|
(let ((server (make-instance 'server)))
  (server-start server)
  (server-stop server))
|#
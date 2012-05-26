(in-package :nunumo)

(defgeneric client-open (client))
(defgeneric client-close (client))
(defgeneric client-read (client))
(defgeneric client-write (client object))
(defgeneric client-flush (client))

(defclass* client ()
  ((host "localhost")
   (port *port*)
   (socket nil)
   (stream nil)))

(defmethod client-open ((client client))
  (setf (socket-of client)
        (usocket:socket-connect (host-of client) (port-of client))
        (stream-of client)
        (usocket:socket-stream (socket-of client)))
  client)

(defmethod client-close ((client client))
  (force-output (stream-of client))
  (usocket:socket-close (socket-of client))
  client)

(defmethod client-read ((client client))
  (with-standard-io-syntax
    (read (stream-of client))))

(defmethod client-write ((client client) object)
  (with-standard-io-syntax
    (print object (stream-of client))))

(defmethod client-flush ((client client))
  (force-output (stream-of client))
  client)

(in-package :nunumo)

(defvar *nunumo* nil "nunumo")

(defun start ()
  (setf *nunumo* (make-instance 'inmemory-nunumo)))

(defun stop ()
  (setf *nunumo* nil))

(defgeneric get (key))
(defgeneric set (key value))
(defgeneric add (key value))
(defgeneric replace (key value))
(defgeneric cas (key old-value new-value))
(defgeneric inc (key))
(defgeneric dec (key))


(defmethod get (key)
  (%get *nunumo* key))

(defmethod set (key value)
  (%set *nunumo* key value))

(defmethod add (key value)
  (unless (get key)
    (set key value)))

(defmethod replace (key value)
  (when (get key)
    (set key value)))

(defmethod cas (key old-value new-value)
  (%cas *nunumo* key old-value new-value))


(defclass* nunumo ()
  ())

(defmethod print-object ((nunumo nunumo) stream)
  (print-unreadable-object (nunumo stream :type t :identity t)))

(defclass* inmemory-nunumo (nunumo)
  ((hash (make-hash-table))))


(defgeneric nunumo-open (nunumo &key default-nunumo))
(defgeneric nunumo-close (nunumo))
(defgeneric %get (nunumo key))
(defgeneric %set (nunumo key value))
(defgeneric %add (nunumo key value))
(defgeneric %replace (nunumo key value))
(defgeneric %cas (nunumo key old-value new-value))
(defgeneric %inc (nunumo key))
(defgeneric %def (nunumo key))

(defmethod nunumo-open ((nunumo nunumo) &key (default-nunumo t))
  (when default-nunumo
    (setf *nunumo* nunumo))
  nunumo)

(defmethod nunumo-close ((nunumo nunumo))
  nunumo)


(defmethod %get ((nunumo inmemory-nunumo) key)
  (gethash key (hash-of nunumo)))

(defmethod %set ((nunumo inmemory-nunumo) key value)
  (setf (gethash key (hash-of nunumo)) value))

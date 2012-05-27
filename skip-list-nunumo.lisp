(in-package :nunumo)

(defun make-skip-list-nunumo (directory)
  (make-instance 'skip-list-nunumo
                 :heap (make-heap directory)))

(defclass* skip-list-nunumo (nunumo)
  ((heap)))

(defmethod %get ((nunumo skip-list-nunumo) key)
)

(defmethod %set ((nunumo skip-list-nunumo) key value)
  )

(defstruct node
  (key nil)
  (val nil)
  (next #() :type simple-vector))


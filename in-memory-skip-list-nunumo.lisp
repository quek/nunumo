(in-package :nunumo)

(defclass* in-memory-skip-list-nunumo (nunumo)
  ((skip-list (make-instance 'in-memory-skip-list))))

(defmethod %get ((nunumo in-memory-skip-list-nunumo) key)
  (aif (get-node (skip-list-of nunumo) key)
       (values (value-of it) t)
       (values nil nil)))

(defmethod %set ((nunumo in-memory-skip-list-nunumo) key value)
  (aif (get-node (skip-list-of nunumo) key)
       (setf (value-of it) value)
       (multiple-value-bind (ok node) (add-node (skip-list-of nunumo) key)
         (declare (ignore ok))
         (setf (value-of node) value))))

(defmethod %add ((nunumo in-memory-skip-list-nunumo) key value)
  (multiple-value-bind (ok node) (add-node (skip-list-of nunumo) key)
    (if ok
        (setf (value-of node) value))))

(defmethod %replace ((nunumo in-memory-skip-list-nunumo) key value)
  (aif (get-node (skip-list-of nunumo) key)
       (setf (value-of it) value)))

(defmethod %cas ((nunumo in-memory-skip-list-nunumo) key old-value new-value)
  (aif (get-node (skip-list-of nunumo) key)
       (sb-ext:compare-and-swap (slot-value it 'value) old-value new-value)))

(let ((nunumo (make-instance 'in-memory-skip-list-nunumo)))
  (nunumo-open nunumo)
  (unwind-protect
       (progn
         (assert (not (get 'hello)))
         (set 'hello 'world)
         (assert (eq 'world (get 'hello)))
         (assert (not (replace 123 999)))
         (assert (add 123 456))
         (assert (not (add 123 321)))
         (assert (= 456 (get 123)))
         (assert (replace 123 999))
         (assert (= 999 (get 123)))
         (assert (eq 'world (cas 'hello 'common 'lisp)))
         (assert (eq 'world (get 'hello)))
         (assert (eq 'world (print (cas 'hello 'world 'lisp))))
         (assert (eq 'lisp (get 'hello))))
    (nunumo-close nunumo)))


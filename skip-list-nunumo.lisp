(in-package :nunumo)

(defclass* skip-list-nunumo (nunumo)
  ((directory)
   (max-height)
   (heap)
   (skip-list)))

(defun make-skip-list-nunumo (directory &key (max-height 8))
  (let ((*heap* (make-heap directory)))
    (make-instance 'skip-list-nunumo
                   :heap *heap*
                   :directory directory
                   :max-height max-height)))

(defmethod skip-list-file ((nunumo skip-list-nunumo))
  (merge-pathnames "skip-list" (directory-of nunumo)))

(defmethod nunumo-open ((nunumo skip-list-nunumo) &key (default-nunumo t))
  (let ((*heap* (heap-of nunumo)))
    (heap-open *heap*)
    (let ((skip-list-file (skip-list-file nunumo)))
      (setf (skip-list-of nunumo)
            (if (probe-file skip-list-file)
                (let ((address (with-open-file (in skip-list-file :element-type '(unsigned-byte 8))
                                 (deserialize in))))
                  (heap-read-object *heap* address))
                (let ((skip-list (make-skip-list *heap* (max-height-of nunumo))))
                  (with-open-file (out skip-list-file :direction :output
                                                      :element-type '(unsigned-byte 8))
                    (serialize (heap-write-object *heap* skip-list) out))
                  skip-list)))))
  (call-next-method nunumo :default-nunumo default-nunumo))

(defmethod nunumo-close ((nunumo skip-list-nunumo))
  (let ((*heap* (heap-of nunumo)))
    (heap-close *heap*)
    (call-next-method nunumo)))



(defmethod %get ((nunumo skip-list-nunumo) key)
  (let ((*heap* (heap-of nunumo)))
    (aif (get-node (skip-list-of nunumo) key)
         (values (value-of it) t)
         (values nil nil))))

(defmethod %set ((nunumo skip-list-nunumo) key value)
  (let ((*heap* (heap-of nunumo)))
    (aif (get-node (skip-list-of nunumo) key)
         (setf (value-of it) value)
         (multiple-value-bind (ok node) (add-node (skip-list-of nunumo) key)
           (declare (ignore ok))
           (setf (value-of node) value)))))

(defmethod %replace ((nunumo skip-list-nunumo) key value)
  (let ((*heap* (heap-of nunumo)))
    (aif (get-node (skip-list-of nunumo) key)
         (setf (value-of it) value))))

(defmethod %cas ((nunumo skip-list-nunumo) key old-value new-value)
  (let ((*heap* (heap-of nunumo)))
    (awhen (get-node (skip-list-of nunumo) key)
      (node-lock it)
      (unwind-protect
           (let ((current-value (value-of it)))
             (when (equalp old-value current-value)
               (setf (value-of it) new-value))
             current-value)
        (node-unlock it)))))


(let ((dir "/tmp/test-skip-list-nununmo/"))
  (ignore-errors (sb-ext:delete-directory dir :recursive t))
  (let ((nunumo (make-skip-list-nunumo dir)))
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
           (assert (eq 'world (cas 'hello 'world 'lisp)))
           (assert (eq 'lisp (get 'hello))))
      (nunumo-close nunumo)))
  (let ((nunumo (make-skip-list-nunumo dir)))
    (print "reopen.")
    (nunumo-open nunumo)
    (unwind-protect
         (progn
           (assert (= 999 (get 123)))
           (assert (eq 'lisp (get 'hello)))
           (let ((threads (collect
                              (sb-thread:make-thread
                               (lambda (n)
                                 (declare (ignorable n))
                                 (dotimes (i 1000)
                                   (get (random most-positive-fixnum))
                                   (set (random most-positive-fixnum)
                                        (random most-positive-fixnum))))
                               :arguments (list (scan-range :length 10))))))
             (collect-ignore
              (sb-thread:join-thread (scan threads)))))
      (nunumo-close nunumo))))

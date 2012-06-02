(in-package :nunumo)

(defclass cas-lock-mixin ()
  ((cas-lock :initform nil)))

(defmacro with-cas-lock ((cas-lock-mixin) &body body)
  `(sb-thread::with-cas-lock ((slot-value ,cas-lock-mixin 'cas-lock))
     ,@body))


(defun make-spinlock ()
  (cons nil nil))

(defun lock-spinlock (spinlock)
  (loop while (sb-ext:compare-and-swap (car spinlock) nil t)))

(defun unlock-spinlock (spinlock)
  (setf (car spinlock) nil))

(defmacro with-spinlock ((spinlock) &body body)
  (alexandria:once-only (spinlock)
    `(progn
       (lock-spinlock ,spinlock)
       (unwind-protect
            (progn ,@body)
         (unlock-spinlock ,spinlock)))))


(defun make-recursive-spinlock ()
  (cons nil 0))

(defun lock-recursive-spinlock (recursive-spinlock)
  (loop for ret = (sb-ext:compare-and-swap (car recursive-spinlock) nil sb-thread:*current-thread*)
        until (or (null ret) (eq ret sb-thread:*current-thread*))
        finally (incf (cdr recursive-spinlock))))

(defun unlock-recursive-spinlock (recursive-spinlock)
  (when (zerop (decf (cdr recursive-spinlock)))
    (setf (car recursive-spinlock) nil)))

(defmacro with-recursive-spinlock ((recursive-spinlock) &body body)
  (alexandria:once-only (recursive-spinlock)
    `(progn
       (lock-recursive-spinlock ,recursive-spinlock)
       (unwind-protect
            (progn ,@body)
         (unlock-recursive-spinlock ,recursive-spinlock)))))


(defstruct rw-lock
  (mutex (bordeaux-threads:make-lock))
  (read-count 0)
  (write-lock nil))

(defun read-lock (rw-lock)
  (loop
    (bordeaux-threads:with-lock-held ((rw-lock-mutex rw-lock))
      (unless (rw-lock-write-lock rw-lock)
        (incf (rw-lock-read-count rw-lock))
        (return-from read-lock)))
    (bordeaux-threads:thread-yield)))

(defun write-lock (rw-lock)
  (loop
    (bordeaux-threads:with-lock-held ((rw-lock-mutex rw-lock))
      (unless (and (zerop (rw-lock-read-count rw-lock))
                   (null (rw-lock-write-lock rw-lock)))
        (setf (rw-lock-write-lock rw-lock) t)
        (return)))
    (bordeaux-threads:thread-yield)))

(defun unlock (rw-lock)
  (bordeaux-threads:with-lock-held ((rw-lock-mutex rw-lock))
    (aif (rw-lock-write-lock rw-lock)
         (setf it nil)
         (decf (rw-lock-read-count rw-lock)))))


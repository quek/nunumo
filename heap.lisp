(in-package :nunumo)


(defgeneric heap-open (heap))
(defgeneric heap-close (heap))
(defgeneric heap-alloc (heap size &optional buffer))
(defgeneric heap-free (heap memory))
(defgeneric heap-read (heap memory))
(defgeneric heap-write (heap memory))
(defgeneric heap-lock (heap memory))
(defgeneric heap-unlock (heap memory))

(defmacro with-heap-lock ((heap memory) &body body)
  (alexandria:once-only (heap memory)
    `(progn
       (heap-lock ,heap ,memory)
       (unwind-protect
            (progn ,@body)
         (heap-unlock ,heap ,memory)))))


(alexandria:define-constant +heap-magic-numbe+
    (coerce #(0 0 0 0 1 9 5 8) 'octets)
  :test 'equalp)
(defconstant +heap-header-length+ 8)
(defparameter *mmap-size* (* 1024 1024))
(defconstant +min-block-size+ 16 "16 byte")
(defconstant +block-meta-data-size+ 1 "1 byte. 1 bit 目が +heap-unuse+ の時は未使用")


(defgeneric heap-to-address (heap block-size position))
(defgeneric heap-from-address (heap address))


(defgeneric heap-file-open (heap-file))
(defgeneric heap-file-close (heap-file))
(defgeneric heap-file-read (heap-file position buffer))
(defgeneric heap-file-write (heap-file position buffer))
(defgeneric heap-file-alloc (heap-file))
(defgeneric heap-file-free (heap-file position))
(defgeneric heap-file-initialize (heap-file))
(defgeneric heap-file-header-size (heap-file))
(defgeneric heap-file-lock (heap-file position))
(defgeneric heap-file-unlock (heap-file position))


(defclass* heap (cas-lock-mixin)
  ((directory)
   (heaps)))


(defstruct address
  "シリアライズするために 16 byte のアドレス表現を使う。"
  (segment 0 :type (unsigned-byte 8))
  (offset  0 :type (unsigned-byte 120)))

(defclass* memory ()
  ((address)
   (buffer nil)))


(defun make-heap (directory)
  (ensure-directories-exist directory)
  (make-instance 'heap
                 :directory directory
                 :heaps (collect
                            (let ((block-size (ash +min-block-size+ (scan-range :length 29))))
                             (make-heap-file
                              (merge-pathnames (princ-to-string block-size) directory)
                              block-size)))))

(defmethod heap-open ((heap heap))
  (collect-ignore (heap-file-open (scan (heaps-of heap)))))

(defmethod heap-close ((heap heap))
  (collect-ignore (heap-file-close (scan (heaps-of heap)))))

(defmethod heap-to-address ((heap heap) block-size position)
  (make-address :segment (floor (- (log block-size 2) (log +min-block-size+ 2)))
                :offset (/ (- position +heap-header-length+)
                           (+ block-size +block-meta-data-size+))))

(defmethod heap-from-address ((heap heap) address)
  (let ((block-size (ash +min-block-size+ (address-segment address))))
    (values (find block-size
                  (heaps-of heap)
                  :key #'block-size-of
                  :test #'=)
            (+ (* (address-offset address)
                  (+ block-size +block-meta-data-size+))
               +heap-header-length+))))

(defmethod heap-alloc ((heap heap) size &optional buffer)
  (let ((heap-file (find size
                         (heaps-of heap)
                         :key #'block-size-of
                         :test #'<=)))
    (make-instance 'memory
                   :address (heap-to-address heap
                                             (block-size-of heap-file)
                                             (heap-file-alloc heap-file))
                   :buffer (or buffer (make-buffer size)))))

(defmethod heap-free ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position)
      (heap-from-address heap (address-of memory))
    (heap-file-free heap-file position)))

(defmethod heap-read ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-read heap-file position (buffer-of memory)))
  memory)

(defmethod heap-read ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-read heap-file position (buffer-of memory)))
  memory)

(defmethod heap-write ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-write heap-file position (buffer-of memory)))
  memory)

(defmethod heap-lock ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-lock heap-file position)))

(defmethod heap-unlock ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-unlock heap-file position)))


(defun make-heap-file (file block-size)
  (make-instance 'heap-file
                 :file file
                 :block-size block-size))

(defclass* heap-file (cas-lock-mixin)
  ((file)
   (element-type 'ubyte)
   (element-size 8)
   (stream)
   (free-memories nil)
   (block-size)))


(defmethod heap-file-header-size (heap-file)
  +heap-header-length+)

(defmethod heap-file-open (heap-file)
  (with-slots (stream file element-type) heap-file
    (setf stream
          (open-mmap-stream file *mmap-size* :element-type element-type))
    (if (zerop (stream-length stream))
        (heap-file-initialize heap-file)
        (unless (equal +heap-magic-numbe+
                       (let ((buffer (make-buffer +heap-header-length+)))
                         (read-sequence buffer stream)
                         buffer))
          (error "~a is not heap file." file)))))

(defmethod heap-file-close (heap-file)
  (close (stream-of heap-file)))

(defmethod heap-file-initialize (heap-file)
  (with-slots (stream block-size) heap-file
    (write-sequence +heap-magic-numbe+ stream)))

(defmethod heap-file-open :after ((heap-file heap-file))
  (heap-file-collect-free-memories heap-file))

(defconstant +heap-use+ 1)
(defconstant +heap-unuse+ 0)
(defconstant +heap-locked+ 1)
(defconstant +heap-unlocked+ 0)

(defmethod heap-file-collect-free-memories ((heap-file heap-file))
  "しっぽがフラグ"
  (with-slots (block-size stream free-memories element-size) heap-file
    (let ((offset block-size))
      (setf free-memories
            (loop with stream-length = (stream-length stream)
                  for position = (+ (heap-file-header-size heap-file) offset)
                    then (+ position offset)
                  while (< position stream-length)
                  for byte = (read-byte-at stream position)
                  if (= +heap-unuse+ (logand 1 byte))
                    collect position)))))

(defmethod heap-file-alloc ((heap-file heap-file))
  (with-slots (free-memories stream block-size element-type) heap-file
    (with-cas-lock (heap-file)
      (if free-memories
          (pop free-memories)
          (let ((position (stream-length stream)))
            (write-byte-at stream (+ position block-size) +heap-use+)
            position)))))

(defmethod heap-file-free ((heap-file heap-file) position)
  (with-slots (free-memories stream) heap-file
    (with-cas-lock (heap-file)
      (push position free-memories)
      (write-byte-at stream (+ position (block-size-of heap-file)) +heap-unuse+))))

(defmethod heap-file-read ((heap-file heap-file) position buffer)
  (read-seq-at (stream-of heap-file) buffer position
               :end (min (length buffer)
                         (block-size-of heap-file))))

(defmethod heap-file-write ((heap-file heap-file) position buffer)
  (write-seq-at (stream-of heap-file) buffer position
                :end (min (length buffer)
                          (block-size-of heap-file))))

(defmethod heap-file-lock (heap-file position)
  (with-slots (stream block-size) heap-file
    (let ((position (+ position block-size)))
      (loop
        (with-cas-lock (heap-file)
          (let ((byte (read-byte-at stream position)))
            (when (zerop (logand +heap-locked+ byte))
              (write-byte-at stream position (logior +heap-locked+ byte))
              (return-from heap-file-lock))))
        (sb-thread:thread-yield)))))

(defmethod  heap-file-unlock (heap-file position)
  (with-slots (stream block-size) heap-file
    (let ((position (+ position block-size)))
      (with-cas-lock (heap-file)
        (let ((byte (read-byte-at stream position)))
          (write-byte-at stream position (logxor +heap-locked+  byte)))))))


(let* ((dir (print "/tmp/heap-test/"))
       (heap (progn (ignore-errors (sb-ext:delete-directory dir :recursive t))
                    (make-heap dir))))
  (heap-open heap)
  (unwind-protect
       (progn
         (let ((memory1 (heap-alloc heap 8))
               (memory2 (heap-alloc heap 8)))
           (assert (= 0 (address-segment (address-of memory1))))
           (assert (= 0 (address-offset (address-of memory1))))
           (assert (= 0 (address-segment (address-of memory2))))
           (assert (= 1 (address-offset (address-of memory2))))
           (heap-free heap memory1)
           (let ((memory3 (heap-alloc heap 16)))
             (assert (= 0 (address-segment (address-of memory3))))
             (assert (= 0 (address-offset (address-of memory3)))))))
    (heap-close heap)))

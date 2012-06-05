(in-package :nunumo)


(defgeneric heap-open (heap))
(defgeneric heap-close (heap))
(defgeneric heap-alloc (heap size &optional buffer))
(defgeneric heap-free (heap memory))
(defgeneric heap-read (heap memory))
(defgeneric heap-write (heap memory))
(defgeneric heap-read-object (heap address))
(defgeneric heap-write-object (heap object))
(defgeneric heap-write-object-at (heap object address offset))
(defgeneric heap-serialize-at (heap object address offset))
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
(defconstant +min-block-size+ 8 "8 byte")
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

(defconstant +size-of-address+ 7)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defstruct address
    "シリアライズするために 7 byte のアドレス表現を使う。"
    (segment 0 :type (unsigned-byte 8))
    (offset  0 :type (unsigned-byte 56))))

(alexandria:define-constant +null-address+
    (make-address :segment #xff :offset 0)
  :test 'equalp)

(defun null-address-p (x)
  (equalp x +null-address+))


(defmethod serialize ((self address) stream)
  (write-byte +tag-address+ stream)
  (write-byte (address-segment self) stream)
  (write-integer (address-offset self) (1- +size-of-address+) stream))

(defmethod deserialize-by-tag ((tag (eql +tag-address+)) stream)
  (make-address :segment (read-byte stream)
                :offset (read-integer stream (1- +size-of-address+))))

(defun read-address (stream position)
  (let ((buffer (make-buffer +size-of-address+)))
    (read-seq-at stream buffer position)
    (make-address :segment (svref buffer 0)
                  :offset (from-bytes buffer 1 #.(1- +size-of-address+)))))

(defun write-address (address stream position)
  (let ((buffer (make-buffer +size-of-address+)))
    (setf (svref buffer 0) (address-segment address))
    (bytes-into buffer 1 (address-offset address) #.(1- +size-of-address+))
    (write-seq-at stream buffer position)
    address))


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
  (collect-ignore (heap-file-open (scan (heaps-of heap))))
  heap)

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

(defmethod heap-free ((heap heap) (address address))
  (multiple-value-bind (heap-file position) (heap-from-address heap address)
    (heap-file-free heap-file position)))

(defmethod heap-read ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-read heap-file position (buffer-of memory)))
  memory)

(defmethod heap-read ((heap heap) (address address))
  (multiple-value-bind (heap-file position) (heap-from-address heap address)
    (let ((buffer (make-buffer (block-size-of heap-file))))
      (heap-file-read heap-file position buffer)
      buffer)))

(defmethod heap-read ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-read heap-file position (buffer-of memory)))
  memory)

(defmethod heap-write ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-write heap-file position (buffer-of memory)))
  memory)

(defmethod heap-read-object ((heap heap) (address address))
  (flex:with-input-from-sequence (in (heap-read heap address))
    (deserialize in)))

(defmethod heap-write-object ((heap heap) object)
  (let* ((buffer (flex:with-output-to-sequence (out)
                   (serialize object out)))
         (memory (heap-alloc heap (length buffer) buffer)))
    (heap-write heap memory)
    (address-of memory)))

(defmethod heap-write-object-at ((heap heap) object address offset)
  (let ((object-address (heap-write-object heap object)))
    (heap-serialize-at heap
                      object-address
                      address
                      offset)
    object-address))

(defmethod heap-serialize-at ((heap heap) object address offset)
  (multiple-value-bind (heap-file position) (heap-from-address heap address)
    (heap-file-write heap-file
                     (+ position offset)
                     (flex:with-output-to-sequence (out)
                       (serialize object out)))))

(defmethod heap-write-byte-at ((heap heap) byte address offset)
  (multiple-value-bind (heap-file position) (heap-from-address heap address)
    (heap-file-write-byte heap-file (+ position offset) byte)))

(defmethod heap-lock ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-lock heap-file position)))

(defmethod heap-lock ((heap heap) (address address))
  (multiple-value-bind (heap-file position) (heap-from-address heap address)
    (heap-file-lock heap-file position)))

(defmethod heap-unlock ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-unlock heap-file position)))

(defmethod heap-unlock ((heap heap) (address address))
  (multiple-value-bind (heap-file position) (heap-from-address heap address)
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
          ;; alloc の時に stream-length を使っているので :extend nil にしないと動かない。
          (open-mmap-stream file *mmap-size* :element-type element-type :extend nil))
    (if (zerop (stream-length stream))
        (heap-file-initialize heap-file)
        (unless (equalp +heap-magic-numbe+
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

(defconstant +heap-use-bit+ #x01)
(defconstant +heap-lock-bit+ #x02)

(defmethod heap-file-collect-free-memories ((heap-file heap-file))
  "しっぽがフラグ"
  (with-slots (block-size stream free-memories element-size) heap-file
    (let ((offset (1+ block-size)))
      (setf free-memories
            (loop with stream-length = (stream-length stream)
                  for position = (heap-file-header-size heap-file)
                    then (+ position offset)
                  while (< position stream-length)
                  for byte = (read-byte-at stream (+ position block-size))
                  unless (zerop (logand +heap-use-bit+ byte))
                    collect position)))))

(defmethod heap-file-alloc ((heap-file heap-file))
  (with-slots (free-memories stream block-size element-type) heap-file
    (with-cas-lock (heap-file)
      (let ((position (or (pop free-memories)
                          (stream-length stream))))
        (write-byte-at stream (+ position block-size) +heap-use-bit+)
        position))))

(defmethod heap-file-free ((heap-file heap-file) position)
  (with-slots (free-memories stream) heap-file
    (with-cas-lock (heap-file)
      (push position free-memories)
      (write-byte-at stream (+ position (block-size-of heap-file)) 0))))

(defmethod heap-file-read ((heap-file heap-file) position buffer)
  (read-seq-at (stream-of heap-file) buffer position
               :end (min (length buffer)
                         (block-size-of heap-file))))

(defmethod heap-file-write ((heap-file heap-file) position buffer)
  (write-seq-at (stream-of heap-file) buffer position
                :end (min (length buffer)
                          (block-size-of heap-file))))

(defmethod heap-file-write-byte ((heap-file heap-file) position byte)
  (write-byte-at (stream-of heap-file) position byte))


(defmethod heap-file-lock (heap-file position)
  (with-slots (stream block-size) heap-file
    (let ((position (+ position block-size)))
      (loop
        (with-cas-lock (heap-file)
          (let ((byte (read-byte-at stream position)))
            (when (zerop (logand +heap-lock-bit+ byte))
              (write-byte-at stream position (logior +heap-lock-bit+ byte))
              (return-from heap-file-lock))))
        (sb-thread:thread-yield)))))

(defmethod  heap-file-unlock (heap-file position)
  (with-slots (stream block-size) heap-file
    (let ((position (+ position block-size)))
      (with-cas-lock (heap-file)
        (let ((byte (read-byte-at stream position)))
          (write-byte-at stream position (logxor +heap-lock-bit+ byte)))))))

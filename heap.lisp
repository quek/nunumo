(in-package :nunumo)


(defgeneric heap-open (heap))
(defgeneric heap-close (heap))
(defgeneric heap-alloc (heap size &optional buffer))
(defgeneric heap-free (heap memory))
(defgeneric heap-read (heap memory))
(defgeneric heap-write (heap memory))
(defgeneric heap-lock (heap memory))
(defgeneric heap-unlock (heap memory))


(alexandria:define-constant +heap-magic-numbe+ #(0 0 0 0 1 9 5 8) :test 'equalp)
(defconstant +heap-header-length+ 8)
(defparameter *mmap-size* (* 1024 1024))
(defconstant +min-block-size+ 16 "16 byte")


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
(defgeneric heap-file-lock (heap position))
(defgeneric heap-file-unlock (heap position))


(defclass* heap (cas-lock-mixin)
  ((directory)
   (heaps)))


(defstruct address
  "シリアライズするために 15 byte のアドレス表現を使う。"
  (segment 0 :type (unsigned-byte 8))
  (offset  0 :type (unsigned-byte 112)))

(defclass* memory ()
  ((address)
   (buffer nil)))


(defun make-heap (directory)
  (ensure-directories-exist directory)
  (make-instance 'heap-table
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
                :offset (/ (- position +heap-header-length+) block-size)))

(defmethod heap-from-address ((heap heap) address)
  (let ((block-size (ash +min-block-size+ (address-segment address))))
    (values (find block-size
                  (heaps-of heap)
                  :key #'block-size-of
                  :test #'=)
            (+ (* (address-offset address) block-size) +heap-header-length+))))

(defmethod heap-alloc ((heap heap) size &optional buffer)
  (let ((heap-file (find size
                         (heaps-of heap)
                         :key #'block-size-of
                         :test #'<))) ; block-size の方が1バイト分大きくないといけない。
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
    (heap-file-read heap-file position (memory-buffer memory)))
  memory)

(defmethod heap-write ((heap heap) (memory memory))
  (multiple-value-bind (heap-file position) (heap-from-address heap (address-of memory))
    (heap-file-write heap-file position (memory-buffer memory)))
  memory)


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

(defmethod heap-file-initialize (heap-file)
  (with-slots (stream block-size) heap-file
    (write-sequence +heap-magic-numbe+ stream)))

(defmethod heap-file-open :after ((heap-file heap-file))
  (heap-file-collect-free-memories heap-file))

(defconstant +heap-use+ 1)
(defconstant +heap-unuse+ 0)

(defmethod heap-file-collect-free-memories ((heap-file heap-file))
  "しっぽがフラグ"
  (with-slots (block-size stream free-memories element-size) heap-file
    (let ((offset (1- block-size)))
      (setf free-memories
            (loop for position = (+ (heap-file-header-size heap-file) offset)
                    then (+ position offset)
                  for byte = (ignore-errors (read-byte-at stream position))
                  while byte
                  if (= +heap-unuse+ (logand 1 byte))
                    collect position)))))

(defmethod heap-file-alloc ((heap-file heap-file))
  (with-slots (free-memories stream block-size element-type) heap-file
    (with-cas-lock (heap-file)
      (if free-memories
          (pop free-memories)
          (let ((position (file-length stream)))
            (write-byte-at stream (+ position (1- block-size)) +heap-use+)
            position)))))

(defmethod heap-file-free ((heap-file heap-file) position)
  (with-slots (free-memories stream) heap-file
    (with-cas-lock (heap-file)
      (push position free-memories)
      (write-byte-at stream (+ position (block-size-of heap-file) -1) +heap-unuse+))))

(defmethod heap-file-read ((heap-file heap-file) position buffer)
  (read-seq-at (stream-of heap-file) buffer position
               :end (min (length buffer)
                         (1- (block-size-of heap-file)))))

(defmethod heap-file-write ((heap-file heap-file) position buffer)
  (write-seq-at (stream-of heap-file) buffer position
                :end (min (length buffer)
                          (1- (block-size-of heap-file)))))

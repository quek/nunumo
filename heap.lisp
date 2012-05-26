(in-package :nunumo)


(defgeneric heap-open (heap))
(defgeneric heap-close (heap))
(defgeneric alloc (heap size &optional buffer))
(defgeneric free (memory))
(defgeneric heap-read (heap memory))
(defgeneric heap-write (heap memory))



(defgeneric heap-file-open (heap-file))
(defgeneric heap-file-close (heap-file))
(defgeneric heap-file-read (heap-file position buffer))
(defgeneric heap-file-write (heap-file position buffer))
(defgeneric heap-file-alloc (heap-file))
(defgeneric heap-file-free (heap-file position))
(defgeneric heap-file-initialize (heap-file))


(defclass* heap-lock ()
  ((lock nil)))

(defmacro with-heap-lock ((heap) &body body)
  `(sb-thread::with-cas-lock ((slot-value ,heap 'lock))
     ,@body))


(defclass* heap (heap-lock)
  ((directory)
   (heaps)))

(defstruct memory
  (address 0)
  (block-size)
  (buffer)
  (heap-file))


(defun make-heap (directory)
  (ensure-directories-exist directory)
  (make-instance 'heap-table
                 :directory directory
                 :heaps (collect
                            (let ((block-size (ash 16 (scan-range :length 29))))
                             (make-heap-file
                              (merge-pathnames (princ-to-string block-size) directory)
                              block-size)))))

(defmethod heap-open ((heap heap))
  (collect-ignore (heap-file-open (scan (heaps-of heap)))))

(defmethod heap-close ((heap heap))
  (collect-ignore (heap-file-close (scan (heaps-of heap)))))

(defmethod alloc ((heap heap) size &optional buffer)
  (let ((heap-file (find size
                         (heaps-of heap)
                         :key #'block-size-of
                         :test #'<))) ; block-size の方が1バイト分大きくないといけない。
    (let ((block-size (block-size-of heap-file)))
     (make-memory :address (heap-file-alloc heap-file)
                  :block-size block-size
                  :heap-file heap-file
                  :buffer (or buffer (make-buffer size))))))

(defmethod free ((memory memory))
  (heap-file-free (memory-heap-file memory)
                  (memory-address memory)))

(defmethod heap-read ((heap heap) (memory memory))
  (let ((heap-file (find (memory-block-size memory)
                         (heaps-of heap)
                         :key #'block-size-of
                         :test #'=)))
    (setf (memory-buffer memory)
          (heap-file-read heap-file (free-address-of memory) (memory-buffer memory)))
    memory))



(defun make-heap-file (file block-size)
  (make-instance 'heap-file
                 :file file
                 :block-size block-size))

(defclass* heap-file (heap-lock)
  ((file)
   (element-type 'ubyte)
   (element-size 8)
   (stream)
   (free-memories nil)
   (block-size)))

(defparameter *heap-magic-numbe* #(0 0 0 0 1 9 5 8))

(defmethod heap-file-open (heap-file)
  (with-slots (stream file element-type) heap-file
    (setf stream
          (open-mmap-stream file (* 1024 1024) :element-type element-type))
    (if (zerop (stream-length stream))
        (heap-file-initialize heap-file)
        (unless (equal *heap-magic-numbe*
                       (let ((buffer (make-buffer (length *heap-magic-numbe*))))
                         (read-sequence buffer stream)
                         buffer))
          (error "~a is not heap file." file)))))

(defmethod heap-file-initialize (heap-file)
  (with-slots (stream block-size) heap-file
    (write-sequence *heap-magic-numbe* stream)
    (write-byte-at stream
                   (1- (* block-size 2))
                   1)))

(defmethod heap-file-open :after ((heap-file heap-file))
  (heap-file-collect-free-memories heap-file))

(defmethod heap-file-collect-free-memories ((heap-file heap-file))
  (with-slots (block-size stream free-memories element-size) heap-file
    (let ((offset (1+ block-size)))
      (setf free-memories
            (loop for position = (1- (* block-size 2))
                    then (+ position offset)
                  for byte = (ignore-errors (read-byte-at stream position))
                  while byte
                  if (= 1 (logand 1 byte))
                    collect position)))))

(defmethod heap-file-alloc ((heap-file heap-file))
  (with-slots (free-memories stream block-size element-type) heap-file
    (with-heap-lock (heap-file)
      (if free-memories
          (pop free-memories)
          (let ((position (+ (file-length stream) block-size -1)))
            (write-byte-at stream position 1)
            position)))))

(defmethod heap-file-free ((heap-file heap-file) position)
  (with-slots (free-memories stream) heap-file
    (with-heap-lock (heap-file)
      (push position free-memories)
      (write-byte-at stream (+ position (block-size-of heap-file) -1) 1))))

(defmethod heap-file-read ((heap-file heap-file) position buffer)
  (read-seq-at (stream-of heap-file) buffer position :end (1- (block-size-of heap-file))))

(defmethod heap-file-write ((heap-file heap-file) position buffer)
  (write-seq-at (stream-of heap-file) buffer position :end (min (length buffer)
                                                                (1- (block-size-of heap-file)))))

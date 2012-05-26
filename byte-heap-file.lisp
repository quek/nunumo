(in-package :nunumo)

(defgeneric heap-read-byte (byte-heap-file position))
(defgeneric heap-write-byte (byte-heap-file position byte))

(defun make-byte-heap-file (file byte-size)
  "byte-size より 1bit 小さい bit までしか入りません。"
  (make-instance 'byte-heap-file
                 :file file
                 :element-type `(unsigned-byte ,byte-size)
                 :element-size byte-size
                 :block-size byte-size))

(defclass* byte-heap-file (heap-file)
  ())

(defmethod heap-file-collect-free-memories ((heap-file byte-heap-file))
  (with-slots (block-size stream free-memories element-size) heap-file
    (setf free-memories
          (loop for position = block-size
                  then (+ position block-size)
                for byte = (ignore-errors (read-byte-at stream position))
                while byte
                if (= 1 (logand 1 byte))
                  collect position))))

(defmethod heap-alloc ((heap byte-heap-file))
  (with-slots (free-memories stream) heap
    (with-heap-lock (heap)
      (if free-memories
          (pop free-memories)
          (let ((position (file-length stream)))
            ;; write するまでは未使用フラグをたてたままにする。
            (write-byte-at stream position 1)
            position)))))

(defmethod heap-free ((heap byte-heap-file) position)
  (with-slots (free-memories stream) heap
    (with-heap-lock (heap)
      (push position free-memories)
      (write-byte-at stream position 1))))

(defmethod heap-read-byte ((heap byte-heap-file) position)
  (ash (read-byte-at (stream-of heap) position) -1))

(defmethod heap-write-byte ((heap byte-heap-file) position byte)
  (write-byte-at (stream-of heap)
                 position
                 (ash byte 1)))

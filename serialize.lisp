(in-package :nunumo)

(defgeneric serialize (object stream))
(defgeneric deserialize (stream))
(defgeneric deserialize-by-tag (tag stream))


(macrolet ((m (&rest syms)
             (let ((value -1))
               `(progn
                  ,@(mapcar (lambda (x)
                              (if (numberp x)
                                  (setf value (1- x))
                                  `(defconstant ,x ,(incf value))))
                            syms)))))
  (m +tag-ignore+
     +tag-invalid+
     +tag-t+
     +tag-nil+
     +tag--1+
     +tag-0+
     +tag-1+
     #x10
     +tag-positive-integer-8+
     +tag-positive-integer-16+
     +tag-positive-integer-32+
     +tag-positive-integer-64+
     +tag-positive-integer+
     #x20
     +tag-negative-integer-8+
     +tag-negative-integer-16+
     +tag-negative-integer-32+
     +tag-negative-integer-64+
     +tag-negative-integer+
     #x30
     +tag-string+
     #xe0
     +tag-address+
     +tag-node+))

(defmethod serialize ((self (eql t)) stream)
  (write-byte +tag-t+ stream))

(defmethod serialize ((self (eql nil)) stream)
  (write-byte +tag-nil+ stream))

(defmethod serialize ((self (eql -1)) stream)
  (write-byte +tag--1+ stream))

(defmethod serialize ((self (eql 0)) stream)
  (write-byte +tag-0+ stream))

(defmethod serialize ((self (eql 1)) stream)
  (write-byte +tag-1+ stream))

(defmethod serialize ((self integer) stream)
  (let* ((plusp (plusp self))
         (abs (abs self))
         (size (ceiling (integer-length abs) 8)))
    (write-byte
     (case size
       (1 (if plusp +tag-positive-integer-8+ +tag-negative-integer-8+))
       (2 (if plusp +tag-positive-integer-16+ +tag-negative-integer-16+))
       ((3 4) (if plusp +tag-positive-integer-32+ +tag-negative-integer-32+))
       ((5 6 7 8) (if plusp +tag-positive-integer-64+ +tag-negative-integer-64+))
       (t (if plusp +tag-positive-integer+ +tag-negative-integer+)))
     stream)
    (when (< 8 size)
      (serialize size stream))
    (write-integer abs size stream)))

(defmethod serialize ((self string) stream)
  (write-byte +tag-string+ stream)
  (let* ((octets (flex:string-to-octets self :external-format :utf8))
         (length (length octets)))
    (serialize length stream)
    (write-sequence octets stream)))


(defmethod deserialize (stream)
  (let ((tag (read-byte stream)))
    (deserialize-by-tag tag stream)))

(defmethod deserialize-by-tag ((tag (eql +tag-t+)) stream)
  t)

(defmethod deserialize-by-tag ((tag (eql +tag-nil+)) stream)
  nil)

(defmethod deserialize-by-tag ((tag (eql +tag--1+)) stream)
  -1)

(defmethod deserialize-by-tag ((tag (eql +tag-0+)) stream)
  0)

(defmethod deserialize-by-tag ((tag (eql +tag-1+)) stream)
  1)

(defmethod deserialize-by-tag ((tag (eql +tag-positive-integer-8+)) stream)
  (read-byte stream))

(defmethod deserialize-by-tag ((tag (eql +tag-positive-integer-16+)) stream)
  (read-integer stream 2))

(defmethod deserialize-by-tag ((tag (eql +tag-positive-integer-32+)) stream)
  (read-integer stream 4))

(defmethod deserialize-by-tag ((tag (eql +tag-positive-integer-64+)) stream)
  (read-integer stream 8))

(defmethod deserialize-by-tag ((tag (eql +tag-positive-integer+)) stream)
  (let ((size (deserialize stream)))
    (read-integer stream size)))

(defmethod deserialize-by-tag ((tag (eql +tag-negative-integer-8+)) stream)
  (- (read-byte stream)))

(defmethod deserialize-by-tag ((tag (eql +tag-negative-integer-16+)) stream)
  (- (read-integer stream 2)))

(defmethod deserialize-by-tag ((tag (eql +tag-negative-integer-32+)) stream)
  (- (read-integer stream 4)))

(defmethod deserialize-by-tag ((tag (eql +tag-negative-integer-64+)) stream)
  (- (read-integer stream 8)))

(defmethod deserialize-by-tag ((tag (eql +tag-negative-integer+)) stream)
  (let ((size (deserialize stream)))
    (- (read-integer stream size))))

(defmethod deserialize-by-tag ((tag (eql +tag-string+)) stream)
  (let* ((length (deserialize stream))
         (buffer (make-buffer length)))
    (read-sequence buffer stream)
    (flex:octets-to-string buffer :external-format :utf8)))



(iterate ((x (scan '(t nil -1 0 1
                     255
                     65535
                     4294967295
                     18446744073709551615
                     12345678901234567890123456789012345678901234567890
                     -255
                     -65535
                     -4294967295
                     -18446744073709551615
                     -12345678901234567890123456789012345678901234567890
                     "hello"))))
  (flex:with-input-from-sequence
      (in (flex:with-output-to-sequence (out)
            (serialize x out)))
    (assert (equal x (deserialize in)))))


(in-package :nunumo)

(defun scan-byte (var size)
  (declare (optimizable-series-function))
  (scan-fn '(unsigned-byte 8)
           (lambda ()
             (logand (ash var (* (1- size) -8)) #xff))
           (lambda (_)
             (declare (ignore _))
             (decf size)
             (logand (ash var (* (1- size) -8)) #xff))
           (lambda (_)
             (declare (ignore _))
             (zerop size))))

(defun collect-byte (series)
  (declare (optimizable-series-function))
  (collect-fn 'integer (constantly 0)
              (lambda (acc x)
                (+ (ash acc 8) x))
              series))


(defun vector-push-byte-extend (vector var size)
  (iterate ((i (scan-byte var size)))
           (vector-push-extend i vector)))


(defun sym (&rest args)
  (intern (apply #'concatenate 'string
                 (mapcar (lambda (x) (string-upcase (string x))) args))))

(defmacro def-byte-struct (name &body slots)
  (alexandria:with-gensyms (stream buffer)
    (let ((total-size (loop for (a b slot-size array-size) in slots
                            sum (case slot-size
                                  (:array array-size)
                                  (:atomic-int 8)
                                  (t slot-size))))
          (idx -1))
      `(progn
         (defstruct ,name
           ,@(loop for (slot-name slot-init-val slot-size array-size) in slots
                   collect (list slot-name slot-init-val
                                 :type (case slot-size
                                         (:array t)
                                         (:atomic-int t)
                                         (t `(unsigned-byte ,(* 8 slot-size)))))))
         (defun ,(sym "READ-" name) (,name ,stream)
           (let ((,buffer (make-array ,total-size :element-type '(unsigned-byte 8))))
             (read-sequence ,buffer ,stream)
             ,@(loop for (slot-name slot-init-val slot-size array-size) in slots
                     with offset = 0
                     collect
                  (case slot-size
                    (:array
                       `(progn
                         ,@(loop for i from 0 below array-size
                                 collect `(setf (aref (,(sym name "-" slot-name) ,name) ,i)
                                                (aref ,buffer ,offset))
                                 do (incf offset))))
                    (:atomic-int
                       `(setf (atomic-int-value (,(sym name "-" slot-name) ,name))
                              (collect-byte (scan (subseq ,buffer ,offset
                                                          ,(incf offset 8))))))
                    (t
                       `(setf (,(sym name "-" slot-name) ,name)
                              (collect-byte (scan (subseq ,buffer ,offset
                                                          ,(incf offset slot-size))))))))))
         (defun ,(sym "WRITE-" name) (,name ,stream)
           (let ((,buffer (make-array ,total-size :element-type '(unsigned-byte 8))))
             ,@(loop for (slot-name slot-init-val slot-size array-size) in slots
                     collect
                  (case slot-size
                    (:array
                       `(progn
                          ,@(loop for i from 0 below array-size
                                  collect `(setf (aref ,buffer ,(incf idx))
                                                 (aref (,(sym name "-" slot-name) ,name) ,i)))))
                    (:atomic-int
                       `(let ((val (atomic-int-value (,(sym name "-" slot-name) ,name))))
                          (setf ,@(loop for i from 56 downto 0 by 8
                                        append `((aref ,buffer ,(incf idx))
                                                 (ldb (byte 8  ,i) val))))))
                    (t
                       `(let ((val (,(sym name "-" slot-name) ,name)))
                          (setf ,@(loop for i from (* 8 (1- slot-size)) downto 0 by 8
                                        append `((aref ,buffer ,(incf idx))
                                                 (ldb (byte 8  ,i) val))))))))
             (write-sequence ,buffer ,stream)))))))


(defmacro defun-write-var-num ()
  `(defun write-var-num (buf num &optional (start 0))
     (cond
       ((< num (ash 1 7))
        (setf (aref buf start) num)
        1)
       ,@(loop for i from 2 to 10 collect
               `(,(if (= i 10)
                      t
                      `(< num (ash ,i ,(* i 7))))
                      (setf
                        ,@(loop for j from 0 below i append
                          (cond
                            ((= j 0)
                             `((aref buf (+ ,j start))
                               (logior (ash num ,(* (1- i) -7)) #x80)))
                            ((= j (1- i))
                             `((aref buf (+ ,j start))
                               (logand num #x7f)))
                            (t
                             `((aref buf (+ ,j start))
                               (logior (logand (ash num ,(* (- i j 1) -7)) #x7f) #x80))))))
                      ,i)))))
(defun-write-var-num)

(defun read-var-num (buf size &optional (start 0))
  (loop repeat size
        for rp from start
        for c = (aref buf rp)
        for num = (logand c #x7f) then (+ (ash num 7) (logand c #x7f))
        while (<= #x80 c)
        finally (return (values (- rp start -1) num))))

(declaim (inline write-fixnum))
(defun write-fixnum (buf num width &optional (start 0) )
  (iterate ((v (scan-byte num width))
            (i (scan-range)))
           (setf (aref buf (+ i start)) v)))

(declaim (inline read-fixnum))
(defun read-fixnum (buffer width &optional (start 0))
  (collect-byte (subseries (scan buffer) start (+ start width))))

(defmacro n++ (var &optional (delta 1))
  `(prog1 ,var (incf ,var ,delta)))

(defmacro bytes-into (buffer p num size)
  (if (numberp size)
      (alexandria:once-only (num)
        `(setf ,@(loop for i from (* 8 (1- size)) downto 0 by 8
                       for n from 0
                       append `((aref ,buffer ,(if (numberp p) (+ p n) `(incf ,p)))
                                (ldb (byte 8 ,i) ,num)))))
      (alexandria:with-gensyms (i n)
        `(loop for ,i from (* 8 (1- ,size)) downto 0 by 8
               ,@(when (numberp p) `(for ,n from 0))
               do (setf (aref ,buffer ,(if (numberp p) `(+ p ,n) `(incf ,p)))
                        (ldb (byte 8 ,i) ,num))))))

(defmacro from-bytes (buffer start end)
  `(loop for i from ,start below ,end
         for n = (aref ,buffer i) then (+ (ash n 8) (aref ,buffer i))
         finally (return n)))

(defun make-buffer (size)
  (make-array size :element-type '(unsigned-byte 8)))

(defun unsigned-byte-to-vector (unsigned-byte size)
  (let ((buffer (make-buffer size)))
    (loop for i from 0 below size
          do (setf (aref buffer i)
                   (ldb (byte 8 (* i 8)) unsigned-byte)))
    buffer))

(defun vector-to-unsigned-byte (vector size)
  (loop for i from 0 below size
        with n = 0
        do (setf n (dpb (aref vector i) (byte 8 (* i 8)) n))
        finally (return n)))

(macrolet ((def-refs ()
               `(progn
                  ,@(loop for (x y) in '((ref-8 sb-sys:sap-ref-8)
                                         (ref-16 sb-sys:sap-ref-16)
                                         (ref-32 sb-sys:sap-ref-32)
                                         (ref-64 sb-sys:sap-ref-64))
                          collect `(defun ,x (sap offset)
                                     (,y sap offset))
                          collect `(defun (setf ,x) (value sap offset)
                                     (setf (,y sap offset) value))))))
  (def-refs))

(defun gref (x index)
  (typecase x
    (sb-sys:system-area-pointer
       (ref-8 x index))
    (t
       (aref x index))))

(defun copy-sap-to-vector (sap sap-start vector vector-start length)
  (typecase vector
    (simple-vector
       (sb-sys::with-pinned-objects (sap vector)
         (sb-kernel::system-area-ub8-copy sap sap-start
                                          (sb-sys::vector-sap vector) vector-start
                                          length)))
    (t
       (loop repeat length
             for i from sap-start
             for j from vector-start
             do (setf (aref vector j)
                      (sb-sys:sap-ref-8 sap i))))))

(defun copy-vector-to-sap (vector vector-start sap sap-start length)
  (typecase vector
    (simple-vector
       (sb-sys::with-pinned-objects (vector sap)
         (sb-kernel::system-area-ub8-copy (sb-sys::vector-sap vector) vector-start
                                          sap sap-start
                                          length)))
    (t
       (loop repeat length
             for i from vector-start
             for j from sap-start
             do (setf (sb-sys:sap-ref-8 sap j)
                      (aref vector i))))))

(defun copy-sap-to-sap (sap-src sap-src-start sap-dest sap-dest-start length)
  (sb-sys::with-pinned-objects (sap-src sap-dest)
    (sb-kernel::system-area-ub8-copy sap-src sap-src-start
                                     sap-dest sap-dest-start
                                     length)))

(defun string-to-octets (string)
  (sb-ext:string-to-octets string :external-format :utf-8))

(defun octets-to-string (octets &key end)
  (sb-ext:octets-to-string octets :external-format :utf-8 :end end))

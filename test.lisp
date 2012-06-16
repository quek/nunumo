(in-package :nunumo)

;; mmap
(let ((file "/tmp/mmap.txt"))
  (ignore-errors (delete-file file))
  (let ((f (open file :direction :io :element-type '(unsigned-byte 8)
                      :if-exists :overwrite :if-does-not-exist :create)))
    (with-open-stream (m (make-instance 'mmap-stream :base-stream f :mmap-size 5 :ext nil))
      (assert (zerop (stream-length m)))
      (write-sequence (make-buffer 8) m)))
  (let ((f (open file :direction :io :element-type '(unsigned-byte 8)
                      :if-exists :overwrite)))
    (with-open-stream (m (make-instance 'mmap-stream :base-stream f :mmap-size 5 :ext nil))
      (assert (= 8 (stream-length m))))))


;; inmemory-nunumo
(let ((nunumo (make-instance 'inmemory-nunumo)))
  (nunumo-open nunumo)
  (assert (eq 'bar
              (progn (set 'foo 'bar)
                     (get 'foo))))
  (nunumo-close nunumo))



;; heap
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
           (with-open-file (stream (merge-pathnames "8" dir))
             (assert (= #.(+ 8 (* 2 9)) (file-length stream))))
           (heap-free heap memory1)
           (let ((memory3 (heap-alloc heap 8)))
             (assert (= 0 (address-segment (address-of memory3))))
             (assert (= 0 (address-offset (address-of memory3)))))
           (let ((memory4 (heap-alloc heap 8)))
             (assert (= 0 (address-segment (address-of memory4))))
             (assert (= 2 (address-offset (address-of memory4))))
             (with-open-file (stream (merge-pathnames "8" dir))
               (assert (= #.(+ 8 (* 3 9)) (file-length stream)))))
           (setf memory1 (heap-alloc heap (ash +min-block-size+ 20)))
           (assert (= 20 (address-segment (address-of memory1))))
           (assert (= 0 (address-offset (address-of memory1)))))
         (with-open-file (stream (merge-pathnames "128" dir))
           (assert (= #.(+ 8 (* 0 129)) (file-length stream))))
         (heap-alloc heap 128)
         (with-open-file (stream (merge-pathnames "128" dir))
           (assert (= #.(+ 8 (* 1 129)) (file-length stream))))
         (heap-alloc heap 128)
         (with-open-file (stream (merge-pathnames "128" dir))
           (assert (= #.(+ 8 (* 2 129)) (file-length stream))))
         (heap-alloc heap 128)
         (with-open-file (stream (merge-pathnames "128" dir))
           (assert (= #.(+ 8 (* 3 129)) (file-length stream))))
         (let ((address (heap-write-object heap most-positive-fixnum)))
           (assert (= most-positive-fixnum (heap-read-object heap address)))))
    (heap-close heap)))

(print "heap reopen")
(let* ((dir (print "/tmp/heap-test/"))
       (heap (progn (ignore-errors (sb-ext:delete-directory dir :recursive t))
                    (make-heap dir))))
  (heap-open heap)
  (unwind-protect
       (progn
         (let ((address (address-of (heap-alloc heap 128))))
           (assert (= #.(+ 8 128 1) (stream-length (stream-of (nth 4 (heaps-of heap))))))
           (assert (= 0 (address-offset address)))))
    (heap-close heap))
  (heap-open heap)
  (unwind-protect
       (let ((heap-file (nth 4 (heaps-of heap))))
         (assert (null (free-memories-of heap-file))
                 ((free-memories-of heap-file)))
         (assert (= (+ 8 128 1) (stream-length (stream-of heap-file)))
                   ((stream-length (stream-of heap-file))))
         (let ((address (address-of (heap-alloc heap 128))))
           (assert (= (+ 8 128 1 128 1) (stream-length (stream-of heap-file)))
                   ((stream-length (stream-of heap-file))))
           (assert (= 1 (address-offset address))
                   ((address-offset address)))))
    (heap-close heap)))



;; skip-list
(let* ((dir (print "/tmp/test-skip-list/"))
       (*heap* (progn (ignore-errors (sb-ext:delete-directory dir :recursive t))
                      (heap-open (make-heap dir))))
       (skip-list (make-skip-list *heap* 8)))
    (unwind-protect
         (progn
           (add-node skip-list 1 11)
           (assert (= 11 (value-of (get-node skip-list 1))))
           (add-node skip-list 'hello 'world)
           (assert (eq 'world (value-of (get-node skip-list 'hello)))))
      (heap-close *heap*)))


;; skip-list-nunumo
(let ((dir "/tmp/test-skip-list-nununmo/"))
  (ignore-errors (sb-ext:delete-directory dir :recursive t))
  (let ((nunumo (make-skip-list-nunumo dir)))
    (nunumo-open nunumo)
    (nunumo-close nunumo))
  (let ((nunumo (make-skip-list-nunumo dir)))
    (nunumo-open nunumo)
    (print "reopened.")
    (unwind-protect
         (dotimes (i 100)
           (set i t)
           (get i))
      (nunumo-close nunumo))))

;; skip-list-nunumo
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
           (assert (eq 'lisp (get 'hello)))
           (let ((threads (collect
                              (sb-thread:make-thread
                               (lambda (n)
                                 (declare (ignorable n))
                                 (dotimes (i 100)
                                   (get (random 100))
                                   (set (random 100)
                                        (random most-positive-fixnum))))
                               :arguments (list (scan-range :length 10))))))
             (collect-ignore
              (sb-thread:join-thread (scan threads)))))
      (nunumo-close nunumo)))
  (let ((nunumo (make-skip-list-nunumo dir)))
    (print "reopen.")
    (nunumo-open nunumo)
    (unwind-protect
         (progn
           (assert (= 999 (get 123)))
           (assert (eq 'lisp (get 'hello)))
           (set most-positive-fixnum most-positive-fixnum)
           (let ((threads (collect
                              (sb-thread:make-thread
                               (lambda (n)
                                 (declare (ignorable n))
                                 (dotimes (i 10)
                                   (print (get (random most-positive-fixnum)))
                                   (set (random most-positive-fixnum)
                                        (random most-positive-fixnum))))
                               :arguments (list (scan-range :length 1))))))
             (collect-ignore
              (sb-thread:join-thread (scan threads)))))
      (nunumo-close nunumo))))

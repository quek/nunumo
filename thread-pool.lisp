(in-package :nunumo)

(defstruct (thread-pool (:constructor %make-thread-pool))
  (queue)
  (name)
  (stop)
  (sleep-time)
  (threads))

(defun make-thread-pool (number-of-thread &key
                                            name
                                            (sleep-time 0.1))
  (let ((stop (cons nil nil))
        (queue (sb-concurrency:make-queue)))
    (%make-thread-pool
     :name name
     :queue queue
     :stop stop
     :sleep-time sleep-time
     :threads (collect 'bag
                (sb-thread:make-thread
                 (lambda ()
                   (loop until (car stop)
                         do (multiple-value-bind (job ok)
                                (sb-concurrency:dequeue queue)
                              (if ok
                                  (funcall job)
                                  (sleep sleep-time)))))
                 :name (format nil "~a worker thread ~d" name
                               (scan-range :from 1 :upto number-of-thread)))))))

(defun stop-thread-pool (thread-pool)
  (setf (car (thread-pool-stop thread-pool)) t))

(defun join-thread-pool (thread-pool)
  (stop-thread-pool thread-pool)
  (collect-ignore (sb-thread:join-thread (scan (thread-pool-threads thread-pool)))))

(defun add-job (thread-pool job)
  (sb-concurrency:enqueue job (thread-pool-queue thread-pool)))

(defun wait-thread-pool (thread-pool)
  (loop until (sb-concurrency:queue-empty-p (thread-pool-queue thread-pool))
        do (sleep (thread-pool-sleep-time thread-pool))))

(defmacro with-thread-pool ((var number-of-thread
                             &key
                               (name "nunumo::thread-pool")
                               (sleep-time 0.1) )
                            &body body)
  `(let ((,var (make-thread-pool ,number-of-thread
                                :name ,name
                                :sleep-time ,sleep-time)))
     (unwind-protect
          (progn ,@body)
       (stop-thread-pool ,var))))

#|
(labels ((fib (n)
           (if (<= n 2)
               1
               (+ (fib (1- n))
                  (fib (- n 2))))))
  (let ((q (sb-concurrency:make-queue)))
    (with-thread-pool (pool 4)
      (dotimes (i 10)
        (add-job pool (let ((n (+ 31 i)))
                        (lambda ()
                          (sb-concurrency:enqueue (cons n (fib n)) q)))))
      (wait-thread-pool pool)
      (join-thread-pool pool))
    (sb-concurrency:list-queue-contents q)))
;;=> ((31 . 1346269) (32 . 2178309) (33 . 3524578) (34 . 5702887) (35 . 9227465) (36 . 14930352) (37 . 24157817) (38 . 39088169) (39 . 63245986) (40 . 102334155))
|#

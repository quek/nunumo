(in-package :nunumo)

(defclass* in-memory-node ()
  ((key)
   (value nil)
   (top-layer 0)
   (nexts)
   (marked nil)
   (fully-linked nil)
   (lock (make-lock))))

(defclass* in-memory-skip-list ()
  ((head)
   (tail)
   (max-height 4)))

(defmethod initialize-instance :after ((node in-memory-node) &key)
  (setf (nexts-of node) (make-array (1+ (top-layer-of node)) :initial-element nil)))

(defmethod initialize-instance :after ((skip-list in-memory-skip-list) &key)
  (with-slots (head tail max-height) skip-list
    (setf tail (make-instance 'in-memory-node
                              :key *tail-key*
                              :top-layer (1- max-height)
                              :nexts (make-array max-height :initial-element nil)
                              :fully-linked t))
    (setf head (make-instance 'in-memory-node
                              :key *head-key*
                              :top-layer (1- max-height)
                              :nexts (make-array max-height :initial-element tail)
                              :fully-linked t))
    (loop for layer from 0 below max-height
          do (setf (svref (nexts-of head) layer) tail))))


(defmethod find-node ((skip-list in-memory-skip-list) key preds succs)
  (loop with found = nil
        with pred = (head-of skip-list)
        for layer from (1- (max-height-of skip-list)) downto 0
        for curr = (svref (nexts-of pred) layer)
        do (loop while (key< (key-of curr) key)
                 do (setf pred curr
                          curr (svref (nexts-of pred) layer)))
           (if (and (not found) (key= key (key-of curr)))
               (setf found layer))
           (setf (svref preds layer) pred
                 (svref succs layer) curr)
        finally (return found)))

(defmethod random-level ((skip-list in-memory-skip-list))
  (random (max-height-of skip-list)))

(defmethod unlock-preds ((skip-list in-memory-skip-list) preds highest-locked)
  (let ((prev-pred nil))
    (collect-ignore
     (let ((pred (svref preds (scan-range :upto highest-locked))))
       (unless (eq prev-pred pred)
         (setf prev-pred pred)
         (unlock (lock-of pred)))))))

(defmethod add-node ((skip-list in-memory-skip-list) key value)
  (prog ((top-layer (random-level skip-list))
         (preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list))))
   :retry
     (let ((found (find-node skip-list key preds succs)))
       (if found
           ;; 既に同じキーがある場合
           (let ((node-found (svref succs found)))
             (if (marked-of node-found)
                 ;; マークされていたらリトライ
                 (go :retry)
                 (values (loop while (not (fully-linked-of node-found)))
                         node-found)))
           (let ((highest-locked -1))
             (unwind-protect
                  (let ((valid t))
                    (loop with prev-pred = nil
                          for layer from 0 to top-layer
                          while valid
                          for pred = (svref preds layer)
                          for succ = (svref succs layer)
                          do (unless (eq pred prev-pred)
                               (lock (lock-of pred))
                               (setf highest-locked layer
                                     prev-pred pred))
                             (setf valid (and (not (marked-of pred))
                                              (not (marked-of succ))
                                              (eq (svref (nexts-of pred) layer) succ))))
                    (or valid (go :retry))
                    (let ((new-node (make-instance 'in-memory-node
                                                   :key key
                                                   :value value
                                                   :top-layer top-layer)))
                      (loop for layer from 0 to top-layer
                            do (setf (svref (nexts-of new-node) layer)
                                     (svref succs layer))
                               (setf (svref (nexts-of (svref preds layer)) layer)
                                     new-node))
                      (setf (fully-linked-of new-node) t)
                      (return (values new-node new-node))))
               (unlock-preds skip-list preds highest-locked)))))))

(defmethod ok-to-delete-p ((skip-list in-memory-skip-list) candidate found)
  (and (fully-linked-of candidate)
       (= (top-layer-of candidate) found)
       (not (marked-of candidate))))

(defmethod remove-node ((skip-list in-memory-skip-list) key)
  (prog ((node-to-delete nil)
         (marked-p nil)
         (top-layer -1)
         (preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list)))
         (found nil))
   :retry
     (setf found (find-node skip-list key preds succs))
     (if (or marked-p
             (and found (ok-to-delete-p skip-list (svref succs found) found)))
         (progn
           (unless marked-p
             (setf node-to-delete (svref succs found))
             (setf top-layer (top-layer-of node-to-delete))
             (lock (lock-of node-to-delete))
             (when (marked-of node-to-delete)
               (unlock (lock-of node-to-delete))
               (return nil))
             (setf (marked-of node-to-delete) t
                   marked-p t))
           (let ((highest-loked -1))
             (unwind-protect
                  (let ((valid t))
                    (loop with prev-pred = nil
                          for layer from 0 to top-layer
                          while valid
                          for pred = (svref preds layer)
                          for succ = (svref succs layer)
                          do (unless (eq pred prev-pred)
                               (lock (lock-of pred))
                               (setf highest-loked layer
                                     prev-pred pred))
                             (setf valid (and (not (marked-of pred))
                                              (eq (svref (nexts-of pred) layer) succ))))
                    (unless valid
                      (go :retry))
                    (loop for layer from top-layer downto 0
                          do (setf (svref (nexts-of (svref preds layer)) layer)
                                   (svref (nexts-of node-to-delete) layer)))
                    (return t))
               (unlock (lock-of node-to-delete))
               (unlock-preds skip-list preds highest-loked)))))))

(defmethod contain-p ((skip-list in-memory-skip-list) key)
  (let* ((preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list)))
         (found (find-node skip-list key preds succs)))
    (and found
         (let ((succ (svref succs found)))
           (and (fully-linked-of succ)
                (not (marked-of succ))
                succ)))))

(defmethod get-node ((skip-list in-memory-skip-list) key)
  (let* ((preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list)))
         (found (find-node skip-list key preds succs)))
    (and found
         (let ((succ (svref succs found)))
           (and (fully-linked-of succ)
                (not (marked-of succ))
                succ)))))



(let ((skip-list (make-instance 'in-memory-skip-list)))
  (assert (not (contain-p skip-list 10)))
  (assert (add-node skip-list 10 t))
  (assert (not (add-node skip-list 10 t)))
  (assert (contain-p skip-list 10))
  (assert (add-node skip-list 8 t))
  (assert (contain-p skip-list 8))
  (assert (add-node skip-list 12 t))
  (assert (contain-p skip-list 12))
  (assert (remove-node skip-list 10))
  (assert (not (remove-node skip-list 10)))
  (assert (not (contain-p skip-list 10)))
  (assert (add-node skip-list 'hello t))
  (assert (not (add-node skip-list 'hello t)))
  (assert (remove-node skip-list 'hello))
  (assert (not (remove-node skip-list 'hello)))
  (let ((threads (collect
                     (sb-thread:make-thread
                      (lambda (n)
                        (declare (ignorable n))
                        (dotimes (i 1000)
                          (add-node skip-list (random most-positive-fixnum) t)
                          (when (zerop (mod i 7))
                            (remove-node skip-list i))))
                      :arguments (list (scan-range :length 10))))))
    (collect-ignore
     (sb-thread:join-thread (scan threads))))
  (print (collect-length (scan-fn 't
                                  (lambda ()
                                    (head-of skip-list))
                                  (lambda (node)
                                    (svref (nexts-of node) 0))
                                  (lambda (node)
                                    (eq node (tail-of skip-list))))))
  )
(in-package :nunumo)

(defclass* node ()
  ((key)
   (val nil)
   (top-layer 0)
   (nexts)
   (marked nil)
   (fully-linked nil)
   (lock (sb-thread:make-mutex))))

(defclass* skip-list ()
  ((head)
   (tail)
   (max-height 4)))

(defmethod initialize-instance :after ((node node) &key)
  (setf (nexts-of node) (make-array (1+ (top-layer-of node)) :initial-element nil)))

(defmethod initialize-instance :after ((skip-list skip-list) &key)
  (with-slots (head tail max-height) skip-list
    (setf tail (make-instance 'node
                              :key most-positive-fixnum
                              :top-layer (1- max-height)
                              :nexts (make-array max-height :initial-element nil)
                              :fully-linked t))
    (setf head (make-instance 'node
                              :key -1
                              :top-layer (1- max-height)
                              :nexts (make-array max-height :initial-element tail)
                              :fully-linked t))
    (loop for layer from 0 below max-height
          do (setf (svref (nexts-of head) layer) tail))))


(defun lock (lock)
  (sb-thread:grab-mutex lock :waitp nil))

(defun unlock (lock)
  (sb-thread:release-mutex lock))



(defmethod find-node (skip-list key preds succs)
  (loop with found = nil
        with pred = (head-of skip-list)
        for layer from (1- (max-height-of skip-list)) downto 0
        for curr = (svref (nexts-of pred) layer)
        do (loop while (< (key-of curr) key)
                 do (setf pred curr
                          curr (svref (nexts-of pred) layer)))
           (if (and (not found) (= key (key-of curr)))
               (setf found layer))
           (setf (svref preds layer) pred
                 (svref succs layer) curr)
        finally (return found)))

(defmethod random-level (skip-list)
  (random (max-height-of skip-list)))

(defun unlock-preds (preds highest-locked)
  (loop with prev-pred = nil
        for layer from 0 to highest-locked
        for pred = (svref preds layer)
        if (not (eq prev-pred pred))
          do (setf prev-pred pred)
             (unlock (lock-of pred))))

(defmethod add-node (skip-list key)
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
                 (loop while (not (fully-linked-of node-found)))))
           (let ((highest-locked -1))
             (unwind-protect
                  (let ((valid t))
                    (loop with prev-pred = nil
                          for layer from 0 to top-layer
                          while valid
                          for pred = (svref preds layer)
                          for succ = (svref succs layer)
                          do (unless (eq pred prev-pred)
                               (unless (lock (lock-of pred))
                                 (go :retry))
                               (setf highest-locked layer
                                     prev-pred pred))
                             (setf valid (and (not (marked-of pred))
                                              (not (marked-of succ))
                                              (eq (svref (nexts-of pred) layer) succ))))
                    (or valid (go :retry))
                    (let ((new-node (make-instance 'node
                                                   :key key
                                                   :top-layer top-layer)))
                      (loop for layer from 0 to top-layer
                            do (setf (svref (nexts-of new-node) layer)
                                     (svref succs layer))
                               (setf (svref (nexts-of (svref preds layer)) layer)
                                     new-node))
                      (setf (fully-linked-of new-node) t)
                      (return new-node)))
               (unlock-preds preds highest-locked)))))))

(defun ok-to-delete-p (candidate found)
  (and (fully-linked-of candidate)
       (= (top-layer-of candidate) found)
       (not (marked-of candidate))))

(defmethod remove-node (skip-list key)
  (prog ((node-to-delete nil)
         (marked-p nil)
         (top-layer -1)
         (preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list)))
         (found nil))
   :retry
     (setf found (find-node skip-list key preds succs))
     (if (or marked-p
             (and found (ok-to-delete-p (svref succs found) found)))
         (progn
           (unless marked-p
             (setf node-to-delete (svref succs found))
             (setf top-layer (top-layer-of node-to-delete))
             (unless (lock (lock-of node-to-delete))
               (go :retry))
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
                               (unless (lock (lock-of pred))
                                 (go :retry))
                               (setf highest-loked layer
                                     prev-pred pred))
                             (setf valid (and (not (marked-of pred))
                                              (eq (svref (nexts-of pred) layer) succ))))
                    (unless valid
                      (go :retry))
                    (loop for layer from top-layer downto 0
                          do (setf (svref (nexts-of (svref preds layer)) layer)
                                   (svref (nexts-of node-to-delete) layer)))
                    (unlock (lock-of node-to-delete))
                    (return t))
               (unlock-preds preds highest-loked)))))))

(defmethod contain-p (skip-list key)
  (let* ((preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list)))
         (found (find-node skip-list key preds succs)))
    (and found
         (let ((succ (svref succs found)))
           (and (fully-linked-of succ)
                (not (marked-of succ))
                succ)))))


(let ((skip-list (make-instance 'skip-list)))
  (assert (not (contain-p skip-list 10)))
  (assert (add-node skip-list 10))
  (assert (not (add-node skip-list 10)))
  (assert (contain-p skip-list 10))
  (add-node skip-list 8)
  (assert (contain-p skip-list 8))
  (print (add-node skip-list 12))
  (assert (print (contain-p skip-list 12)))
  (assert (remove-node skip-list 10))
  (assert (not (remove-node skip-list 10)))
  (assert (not (contain-p skip-list 10)))
  )
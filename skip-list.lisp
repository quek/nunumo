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
   (max-height 16)))

(defmethod initialize-instance :after ((node node) &key)
  (setf (nexts-of node) (make-array (top-layer-of node) :initial-element nil)))

(defmethod initialize-instance :after ((skip-list skip-list) &key)
  (with-slots (head tail max-height) skip-list
    (setf tail (make-instance 'node
                              :key most-positive-fixnum
                              :top-layer max-height
                              :nexts (make-array max-height :initial-element nil)
                              :fully-linked t))
    (setf head (make-instance 'node
                              :key -1
                              :top-layer max-height
                              :nexts (make-array max-height :initial-element tail)
                              :fully-linked t))))


(defun lock (lock)
  (sb-thread:grab-mutex lock :waitp nil))

(defun recursive-lock (lock)
  (or (sb-thread:holding-mutex-p lock)
      (sb-thread:grab-mutex lock :waitp nil)))

(defun unlock (lock)
  (sb-thread:release-mutex lock))



(defmethod find-node (skip-list key preds succs)
  (let ((found nil)
        (pred (head-of skip-list)))
    (loop for layer from (1- (max-height-of skip-list)) downto 0
          for curr = (svref (nexts-of pred) layer)
          do (loop while (> key (key-of curr))
                   do (setf pred curr
                            curr (svref (nexts-of pred) layer)))
             (if (and (not found) (= key (key-of curr)))
                 (setf found layer))
             (setf (svref preds layer) pred
                   (svref succs layer) curr))
    found))

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
                 (prog1 nil
                   (loop while (not (fully-linked-of node-found))))))
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
                                              (not (make-load-form succ))
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
                      new-node))
               (unlock-preds preds highest-locked)))))))

(in-package :nunumo)

(defvar *heap* nil)

(defgeneric key= (a b)
  (:method (a b)
    (eql a b)))
(defgeneric key< (a b)
  (:method ((a number) (b number))
    (< a b))
  (:method (a b)
    (< (sxhash a) (sxhash b))))
(defgeneric key<= (a b)
  (:method (a b)
    (or (key= a b)
        (key< a b))))
(defgeneric key> (a b)
  (:method (a b)
    (not (key<= a b))))
(defgeneric key>= (a b)
  (:method (a b)
    (not (key< a b))))

(defclass key ()
  ())

(defvar *head-key* (make-instance 'key))
(defvar *tail-key* (make-instance 'key))

(defmethod key< ((a (eql *head-key*)) b)
  t)
(defmethod key< (a (b (eql *head-key*)))
  nil)
(defmethod key< ((a (eql *tail-key*)) b)
  nil)
(defmethod key< (a (b (eql *tail-key*)))
  t)


(defclass* skip-list ()
  ((head nil)
   (tail nil)
   (max-height 4)))

(defun make-root-skip-list (heap max-height)
  (let* ((skip-list (make-instance 'skip-list :max-height max-height ))
         (buffer (flex:with-output-to-sequence (out)
                             (serialize skip-list out)))
         (memory (heap-alloc heap (length buffer) buffer)))
    (heap-write heap memory)
    (values skip-list (address-of memory))))

(defmethod serialize ((self skip-list) stream)
  (write-byte +tag-skip-list+ stream)
  (serialize (head-of self) stream)
  (serialize (tail-of self) stream)
  (serialize (max-height-of self) stream))

(defmethod deserialize-by-tag ((tag (eql +tag-skip-list+)) stream)
  (let* ((head (deserialize stream))
         (tail (deserialize stream))
         (max-height (deserialize stream)))
    (make-instance 'skip-list
                   :head head
                   :tail tail
                   :max-height max-height)))


(defclass* node ()
  ((key)
   (value +null-address+ :type address)
   (value-cache)
   (top-layer 0)
   (nexts)
   (marked nil)
   (fully-linked nil)))

(defmethod serialize ((self node) stream)
  (write-byte +tag-node+ stream)
  (serialize (key-of self) stream)
  (serialize (value-of self) stream)
  (serialize (top-layer-of self) stream)
  (serialize (nexts-of self) stream)
  (serialize (marked-of self) stream)
  (serialize (fully-linked-of self) stream))

(defmethod value-of ((node node))
  (if (slot-boundp node 'value-cache)
      (slot-value node 'value-cache)
      (setf (slot-value node 'value-cache)
            (heap-read-object *heap* (slot-value node 'value)))))

(defmethod (setf value-of) (new-value (node node))
  (with-slots (value) node
    (unless (null-address-p value)
      (heap-free *heap* value))
    (setf value (heap-write-objcet *heap* new-value))))




(defmethod next-node ((skip-list skip-list) node layer)
  )

(defmethod initialize-instance :after ((node node) &key)
  (setf (nexts-of node) (make-array (1+ (top-layer-of node))
                                    :initial-element +null-address+)))

(defmethod initialize-instance :after ((skip-list skip-list) &key)
  (with-slots (head tail max-height) skip-list
    (setf tail (make-instance 'node
                              :key *tail-key*
                              :top-layer (1- max-height)
                              :nexts (make-array max-height :initial-element nil)
                              :fully-linked t))
    (setf head (make-instance 'node
                              :key *head-key*
                              :top-layer (1- max-height)
                              :nexts (make-array max-height :initial-element tail)
                              :fully-linked t))
    (loop for layer from 0 below max-height
          do (setf (svref (nexts-of head) layer) tail))))


(defun lock (lock)
  (sb-thread:grab-mutex lock :waitp t))

(defun unlock (lock)
  (sb-thread:release-mutex lock))



(defmethod find-node (skip-list key preds succs)
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

(defmethod random-level (skip-list)
  (random (max-height-of skip-list)))

(defun unlock-preds (preds highest-locked)
  (let ((prev-pred nil))
    (collect-ignore
     (let ((pred (svref preds (scan-range :upto highest-locked))))
       (unless (eq prev-pred pred)
         (setf prev-pred pred)
         (unlock (lock-of pred)))))))

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
                    (let ((new-node (make-instance 'node
                                                   :key key
                                                   :top-layer top-layer)))
                      (loop for layer from 0 to top-layer
                            do (setf (svref (nexts-of new-node) layer)
                                     (svref succs layer))
                               (setf (svref (nexts-of (svref preds layer)) layer)
                                     new-node))
                      (setf (fully-linked-of new-node) t)
                      (return (values new-node new-node))))
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

(defmethod get-node (skip-list key)
  (let* ((preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list)))
         (found (find-node skip-list key preds succs)))
    (and found
         (let ((succ (svref succs found)))
           (and (fully-linked-of succ)
                (not (marked-of succ))
                succ)))))





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

(defvar *head-key* '*head-key*)
(defvar *tail-key* '*tail-key*)

(defmethod key< ((a (eql *head-key*)) b)
  t)
(defmethod key< (a (b (eql *head-key*)))
  nil)
(defmethod key< ((a (eql *tail-key*)) b)
  nil)
(defmethod key< (a (b (eql *tail-key*)))
  t)


(defclass* skip-list ()
  ((address)
   (head nil)
   (tail nil)
   (max-height 4)))

(defun make-skip-list (heap max-height)
  (let* ((skip-list (make-instance 'skip-list :max-height max-height))
         (buffer (flex:with-output-to-sequence (out)
                   (serialize skip-list out)))
         (memory (heap-alloc heap (length buffer) buffer)))
    (heap-write heap memory)
    (setf (address-of skip-list) (address-of memory))
    skip-list))

(defmethod serialize ((self skip-list) stream)
  (write-byte +tag-skip-list+ stream)
  (serialize (address-of (head-of self)) stream)
  (serialize (address-of (tail-of self)) stream)
  (serialize (max-height-of self) stream))

(defmethod deserialize-by-tag ((tag (eql +tag-skip-list+)) stream)
  (let* ((head-address (deserialize stream))
         (tail-address (deserialize stream))
         (head (heap-read-object *heap* head-address))
         (tail (heap-read-object *heap* tail-address))
         (max-height (deserialize stream)))
    (setf (address-of head) head-address
          (address-of tail) tail-address)
    (make-instance 'skip-list
                   :head head
                   :tail tail
                   :max-height max-height)))


(defclass* node ()
  ((address)
   (key)
   (value +null-address+ :type address)
   (top-layer 0 :type ubyte)
   (nexts)))

(defgeneric node= (x y)
  (:method (x y)
    nil)
  (:method ((x node) (y node))
    (key= (key-of x) (key-of y))))

(defun make-node (&key key (value nil value-p) top-layer fully-linked)
  (let ((node (make-instance 'node
                             :key key
                             :top-layer top-layer)))
    (setf (address-of node) (heap-write-object *heap* node)
          (fully-linked-of node) fully-linked)
    (when value-p
      (setf (value-of node)  value))
    node))

(defun node-lock (node)
  (heap-lock *heap* (address-of node)))

(defun node-unlock (node)
  (heap-unlock *heap* (address-of node)))


(defconstant +node-value-offset+ 1)
(defconstant +node-marked-offset+ 9)
(defconstant +node-fully-linked-offset+ 10)
(defconstant +node-nexts-offset+ 12)

(defmethod serialize ((self node) stream)
  (write-byte +tag-node+ stream)              ; 0
  (serialize (slot-value self 'value) stream) ; 1
  (serialize nil stream)                      ; 9 marked
  (serialize nil stream)                      ; 10 fully-linked
  (write-byte (top-layer-of self) stream)     ; 11
  (loop with nexts = (slot-value self 'nexts) ; 12
        for i to (top-layer-of self)
        do (serialize (svref nexts i) stream))
  (serialize (key-of self) stream))

(defmethod deserialize-by-tag ((tag (eql +tag-node+)) stream)
  (let* ((value (deserialize stream))
         (top-layer (progn
                      (read-byte stream) ; marked
                      (read-byte stream) ; fully-linked
                      (read-byte stream)))
         (node (make-instance 'node
                             :value value
                             :top-layer top-layer)))
    (loop with nexts = (slot-value node 'nexts)
          for i to (top-layer-of node)
          do (setf (svref nexts i) (deserialize stream)))
    (setf (key-of node) (deserialize stream))
    node))

(defmethod value-of ((node node))
  (heap-read-object *heap* (slot-value node 'value)))

(defmethod (setf value-of) (new-value (node node))
  (with-slots (value address) node
    (unless (null-address-p value)
      (heap-free *heap* value))
    (setf value (heap-write-object-at *heap* new-value address +node-value-offset+))
    new-value))

(defmethod next-node (node layer)
  (with-slots (nexts) node
    (let* ((address (svref nexts layer))
           (node (heap-read-object *heap* address)))
      (setf (address-of node) address)
      node)))

(defmethod (setf next-node) (new-node (node node) layer)
  (with-slots (nexts address) node
    (let ((new-address (address-of new-node)))
      (heap-serialize-at *heap* new-address address (+ +node-nexts-offset+
                                                       (* 8 layer)))
      (setf (svref nexts layer) new-address)))
  new-node)

(defmethod marked-of ((node node))
  (heap-deserialize-at *heap* (address-of node) +node-marked-offset+))

(defmethod (setf marked-of) (new-value (node node))
  (heap-serialize-at *heap* new-value (address-of node) +node-marked-offset+)
  new-value)

(defmethod fully-linked-of ((node node))
  (heap-deserialize-at *heap* (address-of node) +node-fully-linked-offset+))

(defmethod (setf fully-linked-of) (new-value (node node))
  (heap-serialize-at *heap* new-value (address-of node) +node-fully-linked-offset+)
  new-value)

(defmethod initialize-instance :after ((node node) &key)
  (with-slots (nexts) node
    (setf nexts (make-array (1+ (top-layer-of node))
                            :initial-element +null-address+))))

(defmethod initialize-instance :after ((skip-list skip-list) &key)
  (with-slots (head tail max-height) skip-list
    (unless tail
      (setf tail (make-node :key *tail-key*
                            :top-layer (1- max-height)))
      (setf (fully-linked-of tail) t))
    (unless head
      (setf head (make-node :key *head-key*
                            :top-layer (1- max-height)))
      (setf (fully-linked-of head) t)
      (loop for layer from 0 below max-height
            do (setf (next-node head layer) tail)))))


(defmethod find-node ((skip-list skip-list) key preds succs)
  (loop with found = nil
        with pred = (head-of skip-list)
        for layer from (1- (max-height-of skip-list)) downto 0
        for curr = (next-node pred layer)
        do (loop while (key< (key-of curr) key)
                 do (setf pred curr
                          curr (next-node pred layer)))
           (if (and (not found) (key= key (key-of curr)))
               (setf found layer))
           (setf (svref preds layer) pred
                 (svref succs layer) curr)
        finally (return found)))

(defmethod random-level ((skip-list skip-list))
  (random (max-height-of skip-list)))

(defmethod unlock-preds ((skip-list skip-list) preds highest-locked)
  (let ((prev-pred nil))
    (collect-ignore
     (let ((pred (svref preds (scan-range :upto highest-locked))))
       (unless (node= prev-pred pred)
         (setf prev-pred pred)
         (node-unlock pred))))))

(defmethod add-node ((skip-list skip-list) key value)
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
                          do (unless (node= pred prev-pred)
                               (node-lock pred)
                               (setf highest-locked layer
                                     prev-pred pred))
                             (setf valid (and (not (marked-of pred))
                                              (not (marked-of succ))
                                              (node= (next-node pred layer) succ))))
                    (or valid (go :retry))
                    (let ((new-node (make-node :key key
                                               :value value
                                               :top-layer top-layer)))
                      (loop for layer from 0 to top-layer
                            do (setf (next-node new-node layer)
                                     (svref succs layer))
                               (setf (next-node (svref preds layer) layer)
                                     new-node))
                      (setf (fully-linked-of new-node) t)
                      (return (values new-node new-node))))
               (unlock-preds skip-list preds highest-locked)))))))

(defmethod ok-to-delete-p ((skip-list skip-list) candidate found)
  (and (fully-linked-of candidate)
       (= (top-layer-of candidate) found)
       (not (marked-of candidate))))

(defmethod remove-node ((skip-list skip-list) key)
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
             (node-lock node-to-delete)
             (when (marked-of node-to-delete)
               (node-unlock node-to-delete)
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
                          do (unless (node= pred prev-pred)
                               (node-lock pred)
                               (setf highest-loked layer
                                     prev-pred pred))
                             (setf valid (and (not (marked-of pred))
                                              (node= (next-node pred layer) succ))))
                    (unless valid
                      (go :retry))
                    (loop for layer from top-layer downto 0
                          do (setf (next-node (svref preds layer) layer)
                                   (next-node node-to-delete layer)))
                    (return t))
               (node-unlock node-to-delete)
               (unlock-preds skip-list preds highest-loked)))))))

(defmethod contain-p ((skip-list skip-list) key)
  (let* ((preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list)))
         (found (find-node skip-list key preds succs)))
    (and found
         (let ((succ (svref succs found)))
           (and (fully-linked-of succ)
                (not (marked-of succ))
                succ)))))

(defmethod get-node ((skip-list skip-list) key)
  (let* ((preds (make-array (max-height-of skip-list)))
         (succs (make-array (max-height-of skip-list)))
         (found (find-node skip-list key preds succs)))
    (and found
         (let ((succ (svref succs found)))
           (and (fully-linked-of succ)
                (not (marked-of succ))
                succ)))))



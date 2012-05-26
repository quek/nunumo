(in-package :nunumo)

(defgeneric serialize (object stream))
(defgeneric deserializing (stream))


(macrolet ((m (&rest syms)
             (let ((value -1))
               `(progn
                  ,@(mapcar (lambda (x)
                              `(defconstant ,x ,(incf value)))
                            syms)))))
  (m +invalid+
    +ignore+
    +t+
    +nil+
    +-1+
    +0+
    +1+
    +positive-integer-8+))

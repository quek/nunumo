(in-package :nunumo)

(let ((nunumo (make-instance 'inmemory-nunumo)))
  (nunumo-open nunumo)
  (assert (eq 'bar
              (progn (set 'foo 'bar)
                     (get 'foo))))
  (nunumo-close nunumo))



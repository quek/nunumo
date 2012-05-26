(in-package :nunumo)

(start)

(assert (eq 'bar
            (progn (set 'foo 'bar)
                   (get 'foo))))


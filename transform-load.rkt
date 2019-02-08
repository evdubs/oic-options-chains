#lang racket/base

(require db
         html-parsing
         racket/cmdline
         racket/list
         racket/port
         racket/sequence
         racket/set
         racket/string
         srfi/19 ; Time Data Types and Procedures
         sxml
         threading)

(struct option
  (underlying
   expiration
   strike
   call-put
   bid
   ask
   vol
   delta
   gamma
   theta
   vega
   rho)
  #:transparent)

(define (extract-option xexp offset)
  (option
   (map (λ (i) (second (regexp-match #px"([0-9A-Z]+) *?[0-9]{6}[CP][0-9]{8}" (second i))))
        ((sxpath `((td ,(+ 3 offset)) span a @ onmouseover)) xexp))
   (map (λ (i) (second (regexp-match #px"[0-9A-Z]+ *?([0-9]{6})[CP][0-9]{8}" (second i))))
        ((sxpath `((td ,(+ 3 offset)) span a @ onmouseover)) xexp))
   (map (λ (i) (second (regexp-match #px"[0-9A-Z]+ *?[0-9]{6}[CP]([0-9]{8})" (second i))))
        ((sxpath `((td ,(+ 3 offset)) span a @ onmouseover)) xexp))
   (map (λ (i) (second (regexp-match #px"[0-9A-Z]+ *?[0-9]{6}([CP])[0-9]{8}" (second i))))
        ((sxpath `((td ,(+ 3 offset)) span a @ onmouseover)) xexp))
   (map (λ (i) (third i)) ((sxpath `((td ,(+ 5 offset)) span)) xexp))
   (map (λ (i) (third i)) ((sxpath `((td ,(+ 6 offset)) span)) xexp))
   (map (λ (i) (third i)) ((sxpath `((td ,(+ 10 offset)) span)) xexp))
   (map (λ (i) (third i)) ((sxpath `((td ,(+ 11 offset)) span)) xexp))
   (map (λ (i) (third i)) ((sxpath `((td ,(+ 12 offset)) span)) xexp))
   (map (λ (i) (third i)) ((sxpath `((td ,(+ 13 offset)) span)) xexp))
   (map (λ (i) (third i)) ((sxpath `((td ,(+ 15 offset)) span)) xexp))
   (map (λ (i) (third i)) ((sxpath `((td ,(+ 16 offset)) span)) xexp))))

(define (flatten-option o)
  (option
   (first (option-underlying o))
   (string->date (first (option-expiration o)) "~y~m~d")
   (/ (string->number (first (option-strike o))) 1000)
   (first (option-call-put o))
   (string->number (first (option-bid o)) 10 'number-or-false 'decimal-as-exact)
   (string->number (first (option-ask o)) 10 'number-or-false 'decimal-as-exact)
   (/ (string->number (string-replace (first (option-vol o)) "%" "") 10 'number-or-false 'decimal-as-exact) 100)
   (string->number (first (option-delta o)) 10 'number-or-false 'decimal-as-exact)
   (string->number (first (option-gamma o)) 10 'number-or-false 'decimal-as-exact)
   (string->number (first (option-theta o)) 10 'number-or-false 'decimal-as-exact)
   (string->number (first (option-vega o)) 10 'number-or-false 'decimal-as-exact)
   (string->number (first (option-rho o)) 10 'number-or-false 'decimal-as-exact)))

(define (closest-expiration date options)
  (foldl (λ (o closest)
           (if (< (abs (time-second (time-difference (date->time-utc date) (date->time-utc (option-expiration o)))))
                  (abs (time-second (time-difference (date->time-utc date) (date->time-utc (option-expiration closest))))))
               o
               closest))
         (first options)
         (rest options)))

(define (closest-strike strike options)
  (foldl (λ (o closest)
           (if (< (abs (- strike (option-strike o))) (abs (- strike (option-strike closest))))
               o
               closest))
         (first options)
         (rest options)))

(define base-folder (make-parameter "/var/tmp/oic/options-chains"))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "OIC options chains base folder. Defaults to /var/tmp/oic/options-chains"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "OIC options chains folder date. Defaults to today"
                         (folder-date (string->date date "~Y-~m-~d"))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define insert-counter 0)
(define insert-success-counter 0)
(define insert-failure-counter 0)

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".html")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) ".html" "")])
      (call-with-input-file file-name
        (λ (in) (let* ([xexp (html->xexp (~> (port->string in)
                                             (string-replace _ "\r\n" "")
                                             (string-replace _ "\t" "")
                                             (string-replace _ "&nbsp;" "")
                                             (string-replace _ "<nobr>" "")
                                             (string-replace _ "</nobr>" "")))]
                       [mark-price (~> ((sxpath '(html body table tr td (table 5) (tr 2) (td 1))) xexp)
                                       (first _)
                                       (second _)
                                       (string->number _ 10 'number-or-false 'decimal-as-exact))]
                       [target-strikes (list (* mark-price 96/100) (* mark-price 98/100) mark-price
                                             (* mark-price 102/100) (* mark-price 104/100))]
                       [target-expirations (list (time-utc->date (add-duration (date->time-utc (folder-date))
                                                                               (make-time 'time-duration 0 (* 60 60 24 15))))
                                                 (time-utc->date (add-duration (date->time-utc (folder-date))
                                                                               (make-time 'time-duration 0 (* 60 60 24 30))))
                                                 (time-utc->date (add-duration (date->time-utc (folder-date))
                                                                               (make-time 'time-duration 0 (* 60 60 24 60)))))]
                       [all-options (~> (map (λ (exp-table)
                                               (map (λ (row) (list (extract-option row 0) (extract-option row -1)))
                                                    ((sxpath '(td table tr)) exp-table)))
                                             ((sxpath '(html body table tr td (table 9) tr)) xexp))
                                        (flatten _)
                                        (filter (λ (o) (not (empty? (option-underlying o)))) _)
                                        (map (λ (o) (flatten-option o)) _))]
                       [options (flatten (map (λ (te) (let* ([e (option-expiration (closest-expiration te all-options))]
                                                             [f (filter (λ (o) (equal? e (option-expiration o))) all-options)])
                                                        (map (λ (ts) (let ([s (option-strike (closest-strike ts f))])
                                                                       (filter (λ (o) (equal? s (option-strike o))) f))) target-strikes)))
                                              target-expirations))])
                  (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                              ticker-symbol
                                                                              " for date "
                                                                              (date->string (folder-date) "~1")))
                                               (displayln ((error-value->string-handler) e 1000))
                                               (rollback-transaction dbc)
                                               (set! insert-failure-counter (+ insert-failure-counter (length options))))])
                    (set! insert-counter (+ insert-counter (length options)))
                    (start-transaction dbc)
                    (for-each (λ (o)
                                (query-exec dbc "
insert into oic.option_chain
(
  act_symbol,
  expiration,
  strike,
  call_put,
  date,
  bid,
  ask,
  vol,
  delta,
  gamma,
  theta,
  vega,
  rho
) values (
  $1,
  $2::text::date,
  $3,
  case $4
    when 'C' then 'Call'::oic.call_put
    when 'P' then 'Put'::oic.call_put
  end,
  $5::text::date,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11,
  $12,
  $13
) on conflict (act_symbol, expiration, strike, call_put, date) do nothing;
"
                                            ticker-symbol
                                            (date->string (option-expiration o) "~1")
                                            (option-strike o)
                                            (option-call-put o)
                                            (date->string (folder-date) "~1")
                                            (option-bid o)
                                            (option-ask o)
                                            (option-vol o)
                                            (option-delta o)
                                            (option-gamma o)
                                            (option-theta o)
                                            (option-vega o)
                                            (option-rho o))) options)
                    (commit-transaction dbc)
                    (set! insert-success-counter (+ insert-success-counter (length options))))))))))

(disconnect dbc)

(displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                          (number->string insert-success-counter) " were successful. "
                          (number->string insert-failure-counter) " failed."))

#lang racket/base

(require db
         gregor
         gregor/period
         html-parsing
         json
         racket/cmdline
         racket/list
         racket/port
         racket/sequence
         racket/string
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

(struct history
  (hv-current
   hv-week-ago
   hv-month-ago
   hv-year-high
   hv-year-high-date
   hv-year-low
   hv-year-low-date
   iv-current
   iv-week-ago
   iv-month-ago
   iv-year-high
   iv-year-high-date
   iv-year-low
   iv-year-low-date)
  #:transparent)

(read-decimal-as-inexact #f)

(define (closest-expiration date options)
  (foldl (λ (o closest)
           (if (< (abs (period-ref (period-between date (option-expiration o) '(days)) 'days))
                  (abs (period-ref (period-between date (option-expiration closest) '(days)) 'days)))
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

(define all-options? (make-parameter #f))

(define base-folder (make-parameter "/var/local/oic/options-chains"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket transform-load.2023-11-16.rkt"
 #:once-each
 [("-a" "--all-options") "Save all options instead of the default select strikes and expirations"
                         (all-options? #t)]
 [("-b" "--base-folder") folder
                         "OIC options chains base folder. Defaults to /var/local/oic/options-chains"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "OIC options chains folder date. Defaults to today"
                         (folder-date (iso8601->date date))]
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

(define (get-options ticker-symbol options-json)
  (let* ([mark-price (query-value dbc "
select
  close
from
  polygon.ohlc
where
  date = (select max(date) from polygon.ohlc where act_symbol = $2 and date <= $1::text::date) and
  act_symbol = $2;"
                                  (date->iso8601 (folder-date))
                                  ticker-symbol)]
         [target-strikes (list (* mark-price 70/100) (* mark-price 725/1000) (* mark-price 75/100) (* mark-price 775/1000)
                               (* mark-price 80/100) (* mark-price 825/1000) (* mark-price 85/100) (* mark-price 875/1000)
                               (* mark-price 90/100) (* mark-price 92/100) (* mark-price 94/100) (* mark-price 96/100) (* mark-price 98/100)
                               mark-price (* mark-price 102/100) (* mark-price 104/100) (* mark-price 106/100) (* mark-price 108/100)
                               (* mark-price 110/100) (* mark-price 1125/1000) (* mark-price 115/100) (* mark-price 1175/1000)
                               (* mark-price 120/100) (* mark-price 1225/1000) (* mark-price 125/100) (* mark-price 1275/1000)
                               (* mark-price 130/100))]
         [target-expirations (list (+days (folder-date) (* 7 2))
                                   (+days (folder-date) (* 7 4))
                                   (+days (folder-date) (* 7 8)))]
         [all-options (~> (filter-map
                           (λ (o) ;(cond [(= -1 (hash-ref o 'call_iv) (hash-ref o 'put_iv)) #f]
                                        ;[else
                                         (list (option ticker-symbol (iso8601->date (hash-ref o 'expirationdate))
                                                       (hash-ref o 'strike) "C"
                                                       (hash-ref o 'call_bid) (hash-ref o 'call_ask)
                                                       ;(if (= -1 (hash-ref o 'call_iv))
                                                           ;(hash-ref o 'put_iv)
                                                       (hash-ref o 'call_ivint)
                                                       ;)
                                                       (hash-ref o 'call_delta) (hash-ref o 'call_gamma) (hash-ref o 'call_theta)
                                                       (hash-ref o 'call_vega) (hash-ref o 'call_rho))
                                               (option ticker-symbol (iso8601->date (hash-ref o 'expirationdate))
                                                       (hash-ref o 'strike) "P"
                                                       (hash-ref o 'put_bid) (hash-ref o 'put_ask)
                                                       ;(if (= -1 (hash-ref o 'put_iv))
                                                           ;(hash-ref o 'call_iv)
                                                       (hash-ref o 'put_ivint)
                                                       ;) 
                                                       (hash-ref o 'put_delta) (hash-ref o 'put_gamma) (hash-ref o 'put_theta)
                                                       (hash-ref o 'put_vega) (hash-ref o 'put_rho)))
                                         ;])
                                         )
                           options-json)
                          (flatten _))])
    (if (all-options?) all-options
        (flatten (map (λ (te) (let* ([e (option-expiration (closest-expiration te all-options))]
                                     [f (filter (λ (o) (equal? e (option-expiration o))) all-options)])
                                (map (λ (ts) (let ([s (option-strike (closest-strike ts f))])
                                               (filter (λ (o) (equal? s (option-strike o))) f))) target-strikes)))
                      target-expirations)))))

(define insert-counter 0)
(define insert-success-counter 0)
(define insert-failure-counter 0)

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [ticker-symbol (string-replace (string-replace file-name (path->string (current-directory)) "") ".json" "")])
      (call-with-input-file file-name
        (λ (in) (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                            ticker-symbol
                                                                            " for date "
                                                                            (~t (folder-date) "yyyy-MM-dd")))
                                              (displayln e)
                                              (rollback-transaction dbc)
                                              (set! insert-failure-counter (add1 insert-failure-counter)))])
                  (start-transaction dbc)
                  (let* ([options-json (string->jsexpr (port->string in))]
                         [options (get-options ticker-symbol options-json)])
                    (set! insert-counter (+ insert-counter (length options)))
                    (for-each (λ (o)
                                (query-exec dbc "
insert into oic.option_chain (
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
  $8::decimal / 100,
  trunc($9, 4),
  trunc($10, 4),
  trunc($11, 4),
  trunc($12, 4),
  trunc($13, 4)
) on conflict (act_symbol, expiration, strike, call_put, date) do nothing;
"
                                            ticker-symbol
                                            (~t (option-expiration o) "yyyy-MM-dd")
                                            (option-strike o)
                                            (option-call-put o)
                                            (~t (folder-date) "yyyy-MM-dd")
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

(define (append-prior-year target-date day-month-str)
  ; leap year hack
  (define adjusted-day-month-str (if (equal? "29-Feb" day-month-str)
                                     "28-Feb" day-month-str))
  (let ([input-this-year (parse-date (string-append day-month-str "-" (number->string (->year target-date)))
                                     "dd-MMM-yyyy")]
        [input-last-year (parse-date (string-append adjusted-day-month-str "-" (number->string (sub1 (->year target-date))))
                                     "dd-MMM-yyyy")]
        [target-last-year (-years target-date 1)])
    (if (and (date>=? input-this-year target-last-year)
             (date>=? target-date input-this-year))
        input-this-year
        input-last-year)))

(define (get-history html-str date)
  (let* ([xexp (html->xexp (~> html-str
                               (string-replace _ "\r\n" "")
                               (string-replace _ "\t" "")
                               (string-replace _ "&nbsp;" " ")
                               (string-replace _ "<nobr>" "")
                               (string-replace _ "</nobr>" "")))]
         [hv-current (second (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 5) (td 2))) xexp)))]
         [hv-week-ago (second (first ((sxpath '(html body table tr td  (table 2) tr (td 1) table (tr 5) (td 3))) xexp)))]
         ;[hv-month-ago (second (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 5) (td 4))) xexp)))]
         [hv-year-high (second (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 5) (td 4))) xexp)))]
         [hv-year-low (third (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 5) (td 5))) xexp)))]
         [iv-current (second (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 9) (td 2))) xexp)))]
         [iv-week-ago (second (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 9) (td 3))) xexp)))]
         ;[iv-month-ago (second (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 9) (td 4))) xexp)))]
         [iv-year-high (second (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 9) (td 4))) xexp)))]
         [iv-year-low (third (first ((sxpath '(html body table tr td (table 2) tr (td 1) table (tr 9) (td 5))) xexp)))])
    (history hv-current
             hv-week-ago
             null ; hv-month-ago
             (first (string-split hv-year-high " - "))
             (if (or (string-prefix? hv-year-high "0.00%") (string-prefix? hv-year-high "N/A"))
                 null
                 (append-prior-year date (second (string-split hv-year-high " - "))))
             (first (string-split hv-year-low " - "))
             (if (or (string-prefix? hv-year-low "0.00%") (string-prefix? hv-year-low "N/A"))
                 null
                 (append-prior-year date (second (string-split hv-year-low " - "))))
             iv-current
             iv-week-ago
             null ; iv-month-ago
             (first (string-split iv-year-high " - "))
             (if (or (string-prefix? iv-year-high "0.00%") (string-prefix? iv-year-high "N/A"))
                 null
                 (append-prior-year date (second (string-split iv-year-high " - "))))
             (first (string-split iv-year-low " - "))
             (if (or (string-prefix? iv-year-low "0.00%") (string-prefix? iv-year-low "N/A"))
                 null
                 (append-prior-year date (second (string-split iv-year-low " - ")))))))

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".html")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [ticker-symbol (string-replace (string-replace file-name (path->string (current-directory)) "") ".html" "")])
      (call-with-input-file file-name
        (λ (in) (let ([html-str (port->string in)])
                  (cond [(or (string-contains? html-str "No Options found")
                             (string-contains? html-str "SEARCH RESULTS")
                             (string-contains? html-str "Server too busy. Try it later."))
                         (displayln (string-append "Unable to retrieve history for " ticker-symbol))]
                        [else
                         (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                                     ticker-symbol
                                                                                     " for date "
                                                                                     (~t (folder-date) "yyyy-MM-dd")))
                                                       (displayln e)
                                                       (rollback-transaction dbc)
                                                       (set! insert-failure-counter (add1 insert-failure-counter)))])
                           (start-transaction dbc)
                           (let ([hist (get-history html-str (folder-date))])
                             (set! insert-counter (add1 insert-counter))
                             (query-exec dbc "
insert into oic.volatility_history
(
  act_symbol,
  date,
  hv_current,
  hv_week_ago,
  hv_month_ago,
  hv_year_high,
  hv_year_high_date,
  hv_year_low,
  hv_year_low_date,
  iv_current,
  iv_week_ago,
  iv_month_ago,
  iv_year_high,
  iv_year_high_date,
  iv_year_low,
  iv_year_low_date
) values (
  $1,
  $2::text::date,
  case $3
    when 'N/A' then null
    when '0.00' then null
    else trunc($3::text::numeric / 100, 4)
  end,
  case $4
    when 'N/A' then null
    when '0.00' then null
    else trunc($4::text::numeric / 100, 4)
  end,
  case $5
    when 'N/A' then null
    when '0.00' then null
    else trunc($5::text::numeric / 100, 4)
  end,
  case $6
    when 'N/A' then null
    when '0.00' then null
    else trunc($6::text::numeric / 100, 4)
  end,
  case $7
    when 'N/A' then null
    else $7::text::date
  end,
  case $8
    when 'N/A' then null
    when '0.00' then null
    else trunc($8::text::numeric / 100, 4)
  end,
  case $9
    when 'N/A' then null
    when '0.00' then null
    else $9::text::date
  end,
  case $10
    when 'N/A' then null
    when '0.00' then null
    else trunc($10::text::numeric / 100, 4)
  end,
  case $11
    when 'N/A' then null
    when '0.00' then null
    else trunc($11::text::numeric / 100, 4)
  end,
  case $12
    when 'N/A' then null
    when '0.00' then null
    else trunc($12::text::numeric / 100, 4)
  end,
  case $13
    when 'N/A' then null
    when '0.00' then null
    else trunc($13::text::numeric / 100, 4)
  end,
  case $14
    when 'N/A' then null
    else $14::text::date
  end,
  case $15
    when 'N/A' then null
    when '0.00' then null
    else trunc($15::text::numeric / 100, 4)
  end,
  case $16
    when 'N/A' then null
    else $16::text::date
  end
) on conflict (act_symbol, date) do nothing;
"
                                         ticker-symbol
                                         (~t (folder-date) "yyyy-MM-dd")
                                         (string-replace (history-hv-current hist) #rx"[,%]" "")
                                         (string-replace (history-hv-week-ago hist) #rx"[,%]" "")
                                         "N/A" ; (string-replace (history-hv-month-ago hist) #rx"[,%]" "")
                                         (string-replace (history-hv-year-high hist) #rx"[,%]" "")
                                         (if (null? (history-hv-year-high-date hist))
                                             "N/A"
                                             (~t (history-hv-year-high-date hist) "yyyy-MM-dd"))
                                         (string-replace (history-hv-year-low hist) #rx"[,%]" "")
                                         (if (null? (history-hv-year-low-date hist))
                                             "N/A"
                                             (~t (history-hv-year-low-date hist) "yyyy-MM-dd"))
                                         (string-replace (history-iv-current hist) #rx"[,%]" "")
                                         (string-replace (history-iv-week-ago hist) #rx"[,%]" "")
                                         "N/A" ; (string-replace (history-iv-month-ago hist) #rx"[,%]" "")
                                         (string-replace (history-iv-year-high hist) #rx"[,%]" "")
                                         (if (null? (history-iv-year-high-date hist))
                                             "N/A"
                                             (~t (history-iv-year-high-date hist) "yyyy-MM-dd"))
                                         (string-replace (history-iv-year-low hist) #rx"[,%]" "")
                                         (if (null? (history-iv-year-low-date hist))
                                             "N/A"
                                             (~t (history-iv-year-low-date hist) "yyyy-MM-dd")))
                             (commit-transaction dbc)
                             (set! insert-success-counter (add1 insert-success-counter))))])))))))

(disconnect dbc)

(displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                          (number->string insert-success-counter) " were successful. "
                          (number->string insert-failure-counter) " underlying symbols failed."))

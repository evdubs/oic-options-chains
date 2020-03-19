#lang racket/base

(require db
         gregor
         gregor/period
         html-parsing
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
   (parse-date (first (option-expiration o)) "yyMMdd")
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

(define (get-options html-str)
  (let* ([xexp (html->xexp (~> html-str
                               (string-replace _ "\r\n" "")
                               (string-replace _ "\t" "")
                               (string-replace _ "&nbsp;" "")
                               (string-replace _ "<nobr>" "")
                               (string-replace _ "</nobr>" "")))]
         [mark-price (~> ((sxpath '(html body table tr td (table 5) (tr 2) (td 1))) xexp)
                         (first _)
                         (second _)
                         (string->number _ 10 'number-or-false 'decimal-as-exact))]
         [target-strikes (list (* mark-price 80/100) (* mark-price 85/100) (* mark-price 90/100) (* mark-price 93/100)
                               (* mark-price 96/100) (* mark-price 98/100) mark-price (* mark-price 102/100) (* mark-price 104/100)
                               (* mark-price 107/100) (* mark-price 110/100) (* mark-price 115/100) (* mark-price 120/100))]
         [target-expirations (list (+days (folder-date) (* 7 2))
                                   (+days (folder-date) (* 7 4))
                                   (+days (folder-date) (* 7 8)))]
         [all-options (~> (map (λ (exp-table)
                                 (map (λ (row) (list (extract-option row 0) (extract-option row -1)))
                                      ((sxpath '(td table tr)) exp-table)))
                               ((sxpath '(html body table tr td (table 9) tr)) xexp))
                          (flatten _)
                          (filter (λ (o) (not (empty? (option-underlying o)))) _)
                          (map (λ (o) (flatten-option o)) _))])
    (flatten (map (λ (te) (let* ([e (option-expiration (closest-expiration te all-options))]
                                 [f (filter (λ (o) (equal? e (option-expiration o))) all-options)])
                            (map (λ (ts) (let ([s (option-strike (closest-strike ts f))])
                                           (filter (λ (o) (equal? s (option-strike o))) f))) target-strikes)))
                  target-expirations))))

(define (append-prior-year target-date day-month-str)
  (let ([input-this-year (parse-date (string-append day-month-str "-" (number->string (->year target-date)))
                                     "dd-MMM-yyyy")]
        [input-last-year (parse-date (string-append day-month-str "-" (number->string (sub1 (->year target-date))))
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
         [history-table-index (length ((sxpath '(html body table tr td table)) xexp))]
         [hv-current (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 5) (td 2))) xexp)))]
         [hv-week-ago (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 5) (td 3))) xexp)))]
         [hv-month-ago (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 5) (td 4))) xexp)))]
         [hv-year-high (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 5) (td 5))) xexp)))]
         [hv-year-low (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 5) (td 6))) xexp)))]
         [iv-current (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 9) (td 2))) xexp)))]
         [iv-week-ago (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 9) (td 3))) xexp)))]
         [iv-month-ago (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 9) (td 4))) xexp)))]
         [iv-year-high (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 9) (td 5))) xexp)))]
         [iv-year-low (second (first ((sxpath `(html body table tr td (table ,history-table-index) (tr 1) (td 1) table (tr 9) (td 6))) xexp)))])
    (history hv-current
             hv-week-ago
             hv-month-ago
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
             iv-month-ago
             (first (string-split iv-year-high " - "))
             (if (or (string-prefix? iv-year-high "0.00%") (string-prefix? iv-year-high "N/A"))
                 null
                 (append-prior-year date (second (string-split iv-year-high " - "))))
             (first (string-split iv-year-low " - "))
             (if (or (string-prefix? iv-year-low "0.00%") (string-prefix? iv-year-low "N/A"))
                 null
                 (append-prior-year date (second (string-split iv-year-low " - ")))))))

(define base-folder (make-parameter "/var/tmp/oic/options-chains"))

(define folder-date (make-parameter (today)))

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

(define insert-counter 0)
(define insert-success-counter 0)
(define insert-failure-counter 0)

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".html")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) ".html" "")])
      (call-with-input-file file-name
        (λ (in) (let ([html-str (port->string in)])
                  (cond [(or (string-contains? html-str "No Options found")
                             (string-contains? html-str "SEARCH RESULTS")
                             (string-contains? html-str "Server too busy. Try it later.")
                             (not (string-contains? html-str "Implied Volatility is suggested by")))
                         (displayln (string-append "Unable to retrieve options for " ticker-symbol))]
                        [else
                         (let ([options (get-options html-str)]
                               [hist (get-history html-str (folder-date))])
                           (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                                       ticker-symbol
                                                                                       " for date "
                                                                                       (~t (folder-date) "yyyy-MM-dd")))
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
                                         (string-replace (history-hv-current hist) "%" "")
                                         (string-replace (history-hv-week-ago hist) "%" "")
                                         (string-replace (history-hv-month-ago hist) "%" "")
                                         (string-replace (history-hv-year-high hist) "%" "")
                                         (if (null? (history-hv-year-high-date hist))
                                             "N/A"
                                             (~t (history-hv-year-high-date hist) "yyyy-MM-dd"))
                                         (string-replace (history-hv-year-low hist) "%" "")
                                         (if (null? (history-hv-year-low-date hist))
                                             "N/A"
                                             (~t (history-hv-year-low-date hist) "yyyy-MM-dd"))
                                         (string-replace (history-iv-current hist) "%" "")
                                         (string-replace (history-iv-week-ago hist) "%" "")
                                         (string-replace (history-iv-month-ago hist) "%" "")
                                         (string-replace (history-iv-year-high hist) "%" "")
                                         (if (null? (history-iv-year-high-date hist))
                                             "N/A"
                                             (~t (history-iv-year-high-date hist) "yyyy-MM-dd"))
                                         (string-replace (history-iv-year-low hist) "%" "")
                                         (if (null? (history-iv-year-low-date hist))
                                             "N/A"
                                             (~t (history-iv-year-low-date hist) "yyyy-MM-dd")))
                             (commit-transaction dbc)
                             (set! insert-success-counter (+ insert-success-counter (length options)))))])))))))

(disconnect dbc)

(displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                          (number->string insert-success-counter) " were successful. "
                          (number->string insert-failure-counter) " failed."))

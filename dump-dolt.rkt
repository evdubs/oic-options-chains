#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/tmp/dolt/options"))

(define start-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define end-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dolt.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/tmp/dolt/options"
                         (base-folder folder)]
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-date end)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today"
                        (start-date start)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

; option-chain
(for-each (λ (date)
            (define option-chain-file (string-append (base-folder) "/option-chain-" date ".csv"))
            (call-with-output-file option-chain-file
              (λ (out)
                (displayln "act_symbol,expiration,strike,call_put,date,bid,ask,vol,delta,gamma,theta,vega,rho" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  act_symbol::text,
  expiration::text,
  strike::text,
  call_put::text,
  date::text,
  bid::text,
  ask::text,
  vol::text,
  delta::text,
  gamma::text,
  theta::text,
  vega::text,
  rho::text
from
  oic.option_chain
where
  date = $1::text::date
order by
  act_symbol, expiration, strike, call_put;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u --continue option_chain option-chain-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  oic.option_chain
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add option_chain; "
                       "/usr/local/bin/dolt commit -m 'option_chain " (end-date) " update'; /usr/local/bin/dolt push"))

; volatility-history
(for-each (λ (date)
            (define volatility-history-file (string-append (base-folder) "/volatility-history-" date ".csv"))
            (call-with-output-file volatility-history-file
              (λ (out)
                (displayln "act_symbol,date,hv_current,hv_week_ago,hv_month_ago,hv_year_high,hv_year_high_date,hv_year_low,hv_year_low_date,iv_current,iv_week_ago,iv_month_ago,iv_year_high,iv_year_high_date,iv_year_low,iv_year_low_date" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  act_symbol::text,
  date::text,
  coalesce(hv_current::text, ''),
  coalesce(hv_week_ago::text, ''),
  coalesce(hv_month_ago::text, ''),
  coalesce(hv_year_high::text, ''),
  coalesce(hv_year_high_date::text, ''),
  coalesce(hv_year_low::text, ''),
  coalesce(hv_year_low_date::text, ''),
  coalesce(iv_current::text, ''),
  coalesce(iv_week_ago::text, ''),
  coalesce(iv_month_ago::text, ''),
  coalesce(iv_year_high::text, ''),
  coalesce(iv_year_high_date::text, ''),
  coalesce(iv_year_low::text, ''),
  coalesce(iv_year_low_date::text, '')
from
  oic.volatility_history
where
  date = $1::text::date
order by
  act_symbol;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u --continue volatility_history volatility-history-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  oic.volatility_history
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add volatility_history; "
                       "/usr/local/bin/dolt commit -m 'volatility_history " (end-date) " update'; /usr/local/bin/dolt push"))

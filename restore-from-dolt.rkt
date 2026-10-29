#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/local/dolt/options"))

(define start-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define end-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket restore-from-dolt.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/local/dolt/options"
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

(define option-chain-filename (string-append (base-folder) "/option-chain-" (start-date) "-" (end-date) ".csv"))

(system (string-append "cd " (base-folder) "; "
                       "/usr/local/bin/dolt pull; "
                       "/usr/local/bin/dolt sql -r csv -q \"
select
  act_symbol,
  expiration,
  strike,
  call_put,
  date,
  bid,
  ask,
  null as model_value,
  vol,
  delta,
  gamma,
  theta,
  vega,
  rho
from
  option_chain
where
  date >= '" (start-date) "' and
  date <= '" (end-date) "';\" > " option-chain-filename))

(query-exec dbc (string-append "copy oic.option_chain from '" option-chain-filename "' (on_error ignore, header match, format csv);"))

(define volatility-history-filename (string-append (base-folder) "/volatility-history-" (start-date) "-" (end-date) ".csv"))

(system (string-append "cd " (base-folder) "; "
                       "/usr/local/bin/dolt pull; "
                       "/usr/local/bin/dolt sql -r csv -q \"
select
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
from
  volatility_history
where
  date >= '" (start-date) "' and
  date <= '" (end-date) "';\" > " volatility-history-filename))

(query-exec dbc (string-append "copy oic.volatility_history from '" volatility-history-filename
                               "' (on_error ignore, header match, format csv);"))

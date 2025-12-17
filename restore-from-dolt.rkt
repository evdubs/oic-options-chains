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
  vol,
  delta,
  gamma,
  theta,
  vega,
  rho,
  null as model_value
from
  option_chain
where
  date >= '" (start-date) "' and
  date <= '" (end-date) "';\" > " option-chain-filename))

(query-exec dbc (string-append "copy oic.option_chain from '" option-chain-filename "' (on_error ignore, header match, format csv);"))
